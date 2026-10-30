// Package relay implements Relays: raw HTTP relays to non-LLM APIs
// (Tavily, AWS Textract, Azure Speech, ...) served at /relay/{endpoint}/{path}. Each endpoint
// carries one credential, a route list and a usage mapping. Bifrost injects the credential, strips
// the caller's, pins the upstream host, prices each counted call with the mapping and hands the
// result to the Relays hooks, where governance charges the caller's overall budgets
// and logging writes the Relay Logs row. Endpoints are not providers: there is no key
// selection, no routing rule and no model budget on this path.
package relay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	ws "github.com/fasthttp/websocket"
	providerUtils "github.com/maximhq/bifrost/core/providers/utils"
	"github.com/maximhq/bifrost/core/schemas"
)

// endpoint is one configured endpoint with its routes, globs and clients compiled once per
// (re)build. It is immutable once built; an update swaps in a new one.
type endpoint struct {
	cfg             *schemas.RelayConfig
	client          *http.Client // unary client; Timeout bounds the whole response
	streamingClient *http.Client // streaming client; no whole-response timeout
	wsDialer        *ws.Dialer   // upstream WebSocket dialer for the routes served as sessions
	operatorHeaders map[string]string
	// operatorLogHeaders is operatorHeaders with every secret-backed value redacted, for the log row.
	operatorLogHeaders map[string]string
	usage              resolvedUsage // endpoint-level usage mapping
	routes             []compiledRoute
	streamGlobs        []routeGlob
	awsKey             *schemas.BedrockKeyConfig // aws_sigv4 credential in the shape the signer reads
	awsRegion          string
	streamIdle         int // seconds
}

// newEndpoint validates cfg and compiles it. proxy is the effective proxy (the endpoint's own, or
// the inherited global one), resolved by the caller.
func newEndpoint(cfg *schemas.RelayConfig, proxy *schemas.ProxyConfig) (*endpoint, error) {
	cfg = cfg.Clone()
	if cfg.Auth.Type == "" {
		cfg.Auth.Type = schemas.RelayAuthBearer
	}
	if err := cfg.ValidateEndpoint(); err != nil {
		return nil, err
	}
	client, err := newHTTPClient(cfg.Network, proxy)
	if err != nil {
		return nil, err
	}
	wsDialer, err := newWebSocketDialer(cfg.Network, proxy)
	if err != nil {
		return nil, err
	}
	usage := resolveUsage(cfg.Usage, nil)
	usage.ignoreUpstreamCost = cfg.IgnoreUpstreamCost
	usage.eventPaths = usage.collectEventPaths()
	routes := compileRoutesWithCostPolicy(cfg, cfg.IgnoreUpstreamCost)
	for i := range routes {
		routes[i].usage.ignoreUpstreamCost = cfg.IgnoreUpstreamCost
		routes[i].usage.eventPaths = routes[i].usage.collectEventPaths()
	}
	e := &endpoint{
		cfg:                cfg,
		client:             client,
		streamingClient:    providerUtils.BuildStreamingHTTPClient(client),
		wsDialer:           wsDialer,
		operatorHeaders:    lowerHeaderValues(cfg.ExtraHeaders),
		operatorLogHeaders: loggableHeaderValues(cfg.ExtraHeaders),
		usage:              usage,
		routes:             routes,
		streamGlobs:        compileStreamGlobs(cfg),
		streamIdle:         schemas.DefaultEndpointStreamIdleTimeoutSeconds,
	}
	if cfg.Network != nil && cfg.Network.StreamIdleTimeoutInSeconds > 0 {
		e.streamIdle = cfg.Network.StreamIdleTimeoutInSeconds
	}
	if cfg.Auth.Type == schemas.RelayAuthAWSSigV4 && cfg.Credential != nil && cfg.Credential.AWS != nil {
		e.awsKey = bedrockKeyConfig(cfg.Credential.AWS)
		e.awsRegion = cfg.Credential.AWS.Region.GetValue()
	}
	return e, nil
}

// plan is the outcome of matching one request against an endpoint's routes.
type plan struct {
	allowed bool           // false when no route matched and skip_route_check is off
	route   *compiledRoute // nil when no route matched
	index   int            // index into routes, -1 for none
	usage   resolvedUsage
	billing bool
	stream  streamMode
	socket  bool // the matched route serves upgrade requests as WebSocket sessions
}

// streamMode is how a request's reply is relayed.
type streamMode int

const (
	streamAuto streamMode = iota // the reply's Content-Type decides: text/event-stream streams, anything else is read and priced
	streamOn                     // the route or streaming.paths forces the streaming relay
	streamOff                    // buffered: a route that sets stream: false, or one that starts a job
)

func (e *endpoint) plan(method, path string, headers map[string]string) plan {
	route := e.matchRoute(method, path, headers)
	// A disabled route still matches, so its requests are refused instead of reaching a broader route
	// (or the relay-any-path fallback) priced differently.
	p := plan{allowed: (e.cfg.SkipRouteCheck || route != nil) && (route == nil || !route.disabled), route: route, index: -1, usage: e.usage, billing: billableByDefault(method, nil)}
	if route != nil {
		p.usage, p.billing, p.socket = route.usage, route.billable, route.socket
		for i := range e.routes {
			if &e.routes[i] == route {
				p.index = i
				break
			}
		}
	}
	p.stream = e.streamMode(route, path)
	return p
}

// streamMode decides how a request's reply is relayed: a route's explicit flag, then the
// streaming.paths globs, and otherwise the reply itself. Nothing the caller sends (Accept, a
// "stream" flag) selects streaming: a reply that is not an event stream is always read and priced,
// so a caller can never route around response-side billing.
func (e *endpoint) streamMode(route *compiledRoute, path string) streamMode {
	// A job route is always buffered: only the buffered relay reads the start reply, which carries
	// the job id.
	if route != nil && route.job != nil {
		return streamOff
	}
	if route != nil && route.stream != nil {
		if *route.stream {
			return streamOn
		}
		return streamOff
	}
	for _, glob := range e.streamGlobs {
		if glob.match(path) {
			return streamOn
		}
	}
	return streamAuto
}

func isEventStream(contentType string) bool {
	mediaType, _, _ := strings.Cut(contentType, ";")
	return strings.EqualFold(strings.TrimSpace(mediaType), "text/event-stream")
}

// lowerHeaderValues maps the configured extra headers by lower-cased name, for the caller header
// loop and for route header matching, which must see the value that is actually sent.
func lowerHeaderValues(extra map[string]schemas.SecretVar) map[string]string {
	if len(extra) == 0 {
		return nil
	}
	out := make(map[string]string, len(extra))
	for name, value := range extra {
		out[strings.ToLower(name)] = value.GetValue()
	}
	return out
}

// loggableHeaderValues is lowerHeaderValues for the log row: a value the relay reads from the
// environment or a vault is redacted, so a route matcher never copies an operator secret into a row.
func loggableHeaderValues(extra map[string]schemas.SecretVar) map[string]string {
	if len(extra) == 0 {
		return nil
	}
	out := make(map[string]string, len(extra))
	for name, value := range extra {
		if value.IsFromEnv() || value.IsFromVault() {
			out[strings.ToLower(name)] = schemas.RedactedAttrValue
			continue
		}
		out[strings.ToLower(name)] = value.GetValue()
	}
	return out
}

// upstreamRequest is what the relay sends: the caller's request, or a job poll.
type upstreamRequest struct {
	method   string
	path     string
	rawQuery string
	headers  map[string]string
	body     []byte
	internal bool // a job poll: the operator's own request, whose headers keep their x-amz-* names
	socket   bool // a WebSocket upgrade: aws_sigv4 presigns the URL instead of signing headers
}

// buildRequest turns a request into the upstream net/http request: host-pinned URL, caller headers
// minus credentials, operator extra headers, then the credential.
func (e *endpoint) buildRequest(ctx *schemas.BifrostContext, req upstreamRequest) (*http.Request, *schemas.BifrostError) {
	rawQuery, err := e.stripCallerQueryCredential(req.rawQuery)
	if err != nil {
		return nil, providerUtils.NewBifrostBadRequestError(err.Error())
	}
	// The URL is built and validated from the caller's parts alone, so a malformed path or query is
	// refused with an error that carries no credential; the query credential is appended afterwards.
	requestURL, err := providerUtils.BuildPassthroughURL(e.cfg.BaseURL, req.path, rawQuery)
	if err != nil {
		return nil, providerUtils.NewBifrostBadRequestError(err.Error())
	}
	if authQuery := e.appendAuthQuery(""); authQuery != "" {
		separator := "?"
		if rawQuery != "" {
			separator = "&"
		}
		requestURL += separator + authQuery
	}
	httpReq, err := http.NewRequestWithContext(ctx, req.method, requestURL, bytes.NewReader(req.body))
	if err != nil {
		return nil, providerUtils.NewBifrostOperationError("error creating request", err)
	}
	// Headers are applied in a passthrough call's order (SetExtraHeaders, then the caller's safe
	// headers, then the key): the operator's extra headers, the caller's x-bf-eh-* headers over them,
	// the caller's own headers over both, and the credential last, so it always wins. A job poll is
	// the operator's own request and takes no caller headers.
	providerUtils.SetExtraHeadersHTTP(context.Background(), httpReq, e.cfg.ExtraHeaders, nil)
	providerUtils.StripCallerAuthForInsecureURL(requestURL, req.headers)
	if !req.internal {
		for k, values := range callerExtraHeaders(ctx) {
			if e.dropsCallerHeader(k, false, true) {
				continue
			}
			for i, v := range values {
				if i == 0 {
					httpReq.Header.Set(k, v)
				} else {
					httpReq.Header.Add(k, v)
				}
			}
		}
	}
	for k, v := range req.headers {
		if e.dropsCallerHeader(k, req.internal, false) {
			continue
		}
		httpReq.Header.Set(k, v)
	}
	// net/http negotiates gzip and decodes the reply itself only when the request carries no
	// Accept-Encoding of its own; one set here would hand the caller compressed bytes with
	// Content-Encoding stripped and leave usage unreadable. Removed before signing.
	httpReq.Header.Del("Accept-Encoding")
	if req.socket && e.cfg.Auth.Type == schemas.RelayAuthAWSSigV4 {
		// A WebSocket handshake carries its SigV4 signature in the query (the AWS presigned-URL
		// form Transcribe streaming takes), since the signature must survive the upgrade.
		if bifrostErr := e.presignAWSRequest(ctx, httpReq); bifrostErr != nil {
			return nil, bifrostErr
		}
		return httpReq, nil
	}
	if bifrostErr := e.applyAuth(ctx, httpReq); bifrostErr != nil {
		return nil, bifrostErr
	}
	return httpReq, nil
}

// dropsCallerHeader reports whether a caller header is kept off the upstream request. Both kinds
// lose the connection-level and framing headers net/http sets. The caller's own Authorization and
// copy of the relay's credential header are dropped, as passthrough drops the caller's key headers;
// an x-bf-eh-* header keeps them, as SetExtraHeaders does, and the credential applied after every
// caller header replaces them. Under SigV4 only the operation name of the x-amz-* headers is
// forwarded, since each one forwarded is signed with the operator's credentials: x-amz-copy-source
// would read objects outside the pinned bucket and x-amz-acl would publish them. A job poll keeps
// its own x-amz-* names.
func (e *endpoint) dropsCallerHeader(name string, internal, extra bool) bool {
	lower := strings.ToLower(name)
	if _, hop := relayHopByHopHeaders[lower]; hop || lower == "content-length" {
		return true
	}
	if !extra {
		if lower == "authorization" {
			return true
		}
		if authHeader := e.authHeader(); authHeader != "" && strings.EqualFold(lower, authHeader) {
			return true
		}
	}
	return !internal && e.cfg.Auth.Type == schemas.RelayAuthAWSSigV4 && strings.HasPrefix(lower, "x-amz-") && lower != "x-amz-target"
}

// relayHopByHopHeaders are the connection-level names SetExtraHeaders filters out of a passthrough
// call's x-bf-eh-* headers; the transport already drops the caller's own copies.
var relayHopByHopHeaders = map[string]struct{}{
	"connection": {}, "proxy-connection": {}, "keep-alive": {}, "proxy-authenticate": {}, "proxy-authorization": {},
	"te": {}, "trailer": {}, "transfer-encoding": {}, "upgrade": {}, "host": {},
}

// callerExtraHeaders returns the caller's x-bf-eh-* headers the transport admitted (its security
// denylist and header_filter_config applied), prefix stripped and names lower-cased, as a provider
// passthrough reads them.
func callerExtraHeaders(ctx context.Context) map[string][]string {
	if ctx == nil {
		return nil
	}
	extra, _ := ctx.Value(schemas.BifrostContextKeyExtraHeaders).(map[string][]string)
	return extra
}

// routeHeaders returns the caller's headers as the upstream will see them for routing and model
// reading: each x-bf-eh-* header the caller did not also send itself is added beneath them, as a
// passthrough call's caller header replaces its x-bf-eh-* copy. The caller's map is returned as is
// when there are no x-bf-eh-* headers, so the common case allocates nothing.
func routeHeaders(headers map[string]string, extra map[string][]string) map[string]string {
	if len(extra) == 0 {
		return headers
	}
	merged := make(map[string]string, len(headers)+len(extra))
	for name, values := range extra {
		if len(values) > 0 {
			merged[name] = strings.Join(values, ", ")
		}
	}
	maps.Copy(merged, headers)
	return merged
}

// responseHeaders returns the upstream headers the caller may see, with the query credential
// redacted (an upstream that redirects echoes the request query in Location).
func (e *endpoint) responseHeaders(resp *http.Response) map[string]string {
	headers := providerUtils.ExtractPassthroughProviderResponseHeadersFromHTTP(resp)
	redactKeyFromHeaders(headers, e.authQuery(), e.secret())
	return headers
}

// bufferedResult is one relayed call's reply, read in full.
type bufferedResult struct {
	status      int
	headers     map[string]string
	contentType string
	body        []byte
	latency     time.Duration
}

// doBuffered sends one request and reads the reply in full.
func (e *endpoint) doBuffered(ctx *schemas.BifrostContext, req upstreamRequest) (*bufferedResult, *schemas.BifrostError) {
	httpReq, bifrostErr := e.buildRequest(ctx, req)
	if bifrostErr != nil {
		return nil, bifrostErr
	}
	start := time.Now()
	resp, err := providerUtils.DoHTTPRequest(e.client, httpReq)
	if err != nil {
		return nil, transportError(err, time.Since(start))
	}
	return e.readBuffered(resp, start, false)
}

// readBuffered reads an open reply in full. There is no size bound of the relay's own, as there is
// none on a provider's unary reply: the only cap on this path is the one the server puts on the
// request body. bounded applies the endpoint timeout to the read, for a reply opened on the
// streaming client, which has none.
func (e *endpoint) readBuffered(resp *http.Response, start time.Time, bounded bool) (*bufferedResult, *schemas.BifrostError) {
	defer resp.Body.Close()
	var timedOut atomic.Bool
	if bounded {
		timer := time.AfterFunc(max(e.client.Timeout-time.Since(start), 0), func() {
			timedOut.Store(true)
			_ = resp.Body.Close()
		})
		defer timer.Stop()
	}
	// DoHTTPRequest wraps the body so time blocked reading it counts as upstream time too.
	body, err := io.ReadAll(resp.Body)
	latency := time.Since(start)
	if timedOut.Load() {
		return nil, transportError(context.DeadlineExceeded, latency)
	}
	if err != nil {
		return nil, providerUtils.SetErrorLatency(providerUtils.NewBifrostOperationError("error reading response body", err), latency)
	}
	return &bufferedResult{
		status:      resp.StatusCode,
		headers:     e.responseHeaders(resp),
		contentType: resp.Header.Get("Content-Type"),
		body:        body,
		latency:     time.Since(start),
	}, nil
}

// openStream sends one request and returns the open reply for the streaming relay. The streaming
// client has no whole-response deadline, so the wait to connect, send the request and receive the
// headers is bounded here by the endpoint timeout instead; once the reply is open the body is
// governed by the idle-timeout reader. The returned release frees that bound and must be called
// once the reply has been consumed.
func (e *endpoint) openStream(ctx *schemas.BifrostContext, req upstreamRequest) (*http.Response, time.Time, func(), *schemas.BifrostError) {
	httpReq, bifrostErr := e.buildRequest(ctx, req)
	if bifrostErr != nil {
		return nil, time.Time{}, nil, bifrostErr
	}
	release := func() {}
	var timer *time.Timer
	if e.client.Timeout > 0 {
		headerCtx, cancel := context.WithCancelCause(httpReq.Context())
		httpReq = httpReq.WithContext(headerCtx)
		timer = time.AfterFunc(e.client.Timeout, func() { cancel(context.DeadlineExceeded) })
		release = func() {
			timer.Stop()
			cancel(nil)
		}
	}
	start := time.Now()
	resp, err := providerUtils.DoHTTPRequest(e.streamingClient, httpReq)
	if timer != nil && !timer.Stop() && err == nil {
		// The bound fired as the headers arrived: the request context is already cancelled, so
		// the first read would fail. It is the timeout it is, not a cancellation.
		_ = resp.Body.Close()
		release()
		return nil, start, nil, transportError(context.DeadlineExceeded, time.Since(start))
	}
	if err != nil {
		if errors.Is(context.Cause(httpReq.Context()), context.DeadlineExceeded) {
			err = context.DeadlineExceeded
		}
		release()
		return nil, start, nil, transportError(err, time.Since(start))
	}
	return resp, start, release, nil
}
