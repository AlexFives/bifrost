package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fasthttp/router"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	configtables "github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/framework/logstore"
	"github.com/maximhq/bifrost/plugins/logging"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

type usageTestLogManager struct {
	logging.LogManager
	store            logstore.LogStore
	lastModelFilters *logstore.SearchFilters
}

func (m *usageTestLogManager) GetStats(ctx context.Context, filters *logstore.SearchFilters) (*logstore.SearchStats, error) {
	return m.store.GetStats(ctx, *filters)
}

func (m *usageTestLogManager) GetModelRankings(ctx context.Context, filters *logstore.SearchFilters) (*logstore.ModelRankingResult, error) {
	copy := *filters
	m.lastModelFilters = &copy
	return m.store.GetModelRankings(ctx, *filters)
}

type usageFailingLogManager struct{ logging.LogManager }

func (m *usageFailingLogManager) GetStats(context.Context, *logstore.SearchFilters) (*logstore.SearchStats, error) {
	return nil, errors.New("private database detail")
}

func usageRequest(t *testing.T, r *router.Router, key, uri string) *fasthttp.RequestCtx {
	t.Helper()
	var request fasthttp.Request
	request.Header.SetMethod("GET")
	request.SetRequestURI(uri)
	if key != "" {
		request.Header.Set("x-bf-vk", key)
	}
	requestCtx := &fasthttp.RequestCtx{}
	requestCtx.Init(&request, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}, nil)
	r.Handler(requestCtx)
	return requestCtx
}

func TestVirtualKeyUsageSelfService(t *testing.T) {
	SetLogger(&mockLogger{})
	ctx := context.Background()
	store, err := configstore.NewConfigStore(ctx, &configstore.Config{
		Enabled: true, Type: configstore.ConfigStoreTypeSQLite,
		Config: &configstore.SQLiteConfig{Path: filepath.Join(t.TempDir(), "config.db")},
	}, &mockLogger{})
	require.NoError(t, err)
	logs, err := logstore.NewLogStore(ctx, &logstore.Config{
		Enabled: true, Type: logstore.LogStoreTypeSQLite,
		Config: &logstore.SQLiteConfig{Path: filepath.Join(t.TempDir(), "logs.db")},
	}, &mockLogger{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, logs.Close(ctx)) })

	active := true
	expired := time.Now().Add(-time.Hour)
	for _, key := range []configtables.TableVirtualKey{
		{ID: "vk-one", Name: "First", Value: *schemas.NewSecretVar("sk-bf-one"), IsActive: &active},
		{ID: "vk-two", Name: "Second", Value: *schemas.NewSecretVar("sk-bf-two"), IsActive: &active},
		{ID: "vk-expired", Name: "Expired", Value: *schemas.NewSecretVar("sk-bf-expired"), IsActive: &active, ExpiresAt: &expired},
	} {
		require.NoError(t, store.CreateVirtualKey(ctx, &key))
	}
	keyOne, keyTwo := "vk-one", "vk-two"
	costOne, costTwo := 0.25, 9.0
	now := time.Now().UTC()
	for _, entry := range []logstore.Log{
		{ID: "one-recent", Timestamp: now.Add(-time.Hour), Object: "chat_completion", Provider: "openai", Model: "one-model", Status: "success", VirtualKeyID: &keyOne, TotalTokens: 100, Cost: &costOne},
		{ID: "one-old", Timestamp: now.Add(-12 * 24 * time.Hour), Object: "chat_completion", Provider: "openai", Model: "older-model", Status: "success", VirtualKeyID: &keyOne, TotalTokens: 50, Cost: &costOne},
		{ID: "two-recent", Timestamp: now.Add(-time.Hour), Object: "chat_completion", Provider: "openrouter", Model: "private-model", Status: "success", VirtualKeyID: &keyTwo, TotalTokens: 900, Cost: &costTwo},
	} {
		require.NoError(t, logs.Create(ctx, &entry))
	}

	manager := &usageTestLogManager{store: logs}
	h := &GovernanceHandler{configStore: store, logManager: manager}
	r := router.New()
	h.RegisterRoutes(r)

	t.Run("only the presented key is counted despite injected filters", func(t *testing.T) {
		response := usageRequest(t, r, "sk-bf-one", "/api/governance/virtual-keys/usage?period=7d&virtual_key_ids=vk-two")
		require.Equal(t, fasthttp.StatusOK, response.Response.StatusCode(), string(response.Response.Body()))
		require.Contains(t, string(response.Response.Header.Peek("Cache-Control")), "no-store")
		var result struct {
			Period         string               `json:"period"`
			LoggingEnabled bool                 `json:"logging_enabled"`
			Stats          logstore.SearchStats `json:"stats"`
			Models         []struct {
				Model string `json:"model"`
			} `json:"models"`
		}
		require.NoError(t, json.Unmarshal(response.Response.Body(), &result))
		require.Equal(t, "7d", result.Period)
		require.True(t, result.LoggingEnabled)
		require.EqualValues(t, 1, result.Stats.TotalRequests)
		require.EqualValues(t, 100, result.Stats.TotalTokens)
		require.InDelta(t, 0.25, result.Stats.TotalCost, 0.00001)
		require.Len(t, result.Models, 1)
		require.Equal(t, "one-model", result.Models[0].Model)
		require.True(t, manager.lastModelFilters.RequireExactTimeWindow)
		require.NotContains(t, string(response.Response.Body()), "private-model")
		require.NotContains(t, string(response.Response.Body()), "sk-bf-one")
		require.NotContains(t, string(response.Response.Body()), "user_facing_success_rate")
	})

	t.Run("second key sees its own data", func(t *testing.T) {
		response := usageRequest(t, r, "sk-bf-two", "/api/governance/virtual-keys/usage?period=7d")
		require.Equal(t, fasthttp.StatusOK, response.Response.StatusCode(), string(response.Response.Body()))
		require.Contains(t, string(response.Response.Body()), "private-model")
		require.NotContains(t, string(response.Response.Body()), "one-model")
	})

	t.Run("default period includes older requests", func(t *testing.T) {
		response := usageRequest(t, r, "sk-bf-one", "/api/governance/virtual-keys/usage")
		require.Equal(t, fasthttp.StatusOK, response.Response.StatusCode(), string(response.Response.Body()))
		var result struct {
			Period string               `json:"period"`
			Stats  logstore.SearchStats `json:"stats"`
		}
		require.NoError(t, json.Unmarshal(response.Response.Body(), &result))
		require.Equal(t, "30d", result.Period)
		require.EqualValues(t, 2, result.Stats.TotalRequests)
	})

	t.Run("bearer virtual keys authenticate too", func(t *testing.T) {
		var request fasthttp.Request
		request.Header.SetMethod("GET")
		request.Header.Set("Authorization", "Bearer sk-bf-one")
		request.SetRequestURI("/api/governance/virtual-keys/usage?period=24h")
		requestCtx := &fasthttp.RequestCtx{}
		requestCtx.Init(&request, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}, nil)
		r.Handler(requestCtx)
		require.Equal(t, fasthttp.StatusOK, requestCtx.Response.StatusCode(), string(requestCtx.Response.Body()))
	})

	t.Run("self-service route bypasses admin middleware but other routes do not", func(t *testing.T) {
		protected := router.New()
		adminOnly := func(next fasthttp.RequestHandler) fasthttp.RequestHandler {
			return func(ctx *fasthttp.RequestCtx) { ctx.SetStatusCode(fasthttp.StatusUnauthorized) }
		}
		h.RegisterRoutes(protected, adminOnly)
		require.Equal(t, fasthttp.StatusOK, usageRequest(t, protected, "sk-bf-one", "/api/governance/virtual-keys/usage").Response.StatusCode())
		quota := usageRequest(t, protected, "sk-bf-one", "/api/governance/virtual-keys/quota")
		require.Equal(t, fasthttp.StatusOK, quota.Response.StatusCode())
		require.Contains(t, string(quota.Response.Header.Peek("Cache-Control")), "no-store")
		require.Equal(t, fasthttp.StatusUnauthorized, usageRequest(t, protected, "sk-bf-one", "/api/governance/virtual-keys").Response.StatusCode())
	})

	for _, tc := range []struct {
		name, key, uri string
		status         int
	}{
		{"missing key", "", "/api/governance/virtual-keys/usage", fasthttp.StatusUnauthorized},
		{"unknown key", "sk-bf-unknown", "/api/governance/virtual-keys/usage", fasthttp.StatusUnauthorized},
		{"expired key", "sk-bf-expired", "/api/governance/virtual-keys/usage", fasthttp.StatusForbidden},
		{"invalid period", "sk-bf-one", "/api/governance/virtual-keys/usage?period=all", fasthttp.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := usageRequest(t, r, tc.key, tc.uri)
			require.Equal(t, tc.status, response.Response.StatusCode(), string(response.Response.Body()))
		})
	}

	t.Run("disabled logging is reported rather than shown as zero usage", func(t *testing.T) {
		noLogging := router.New()
		(&GovernanceHandler{configStore: store}).RegisterRoutes(noLogging)
		response := usageRequest(t, noLogging, "sk-bf-one", "/api/governance/virtual-keys/usage")
		require.Equal(t, fasthttp.StatusOK, response.Response.StatusCode(), string(response.Response.Body()))
		require.Contains(t, string(response.Response.Body()), `"logging_enabled":false`)
		require.Contains(t, string(response.Response.Body()), `"stats":null`)
	})

	t.Run("log store errors do not expose internal details", func(t *testing.T) {
		failing := router.New()
		(&GovernanceHandler{configStore: store, logManager: &usageFailingLogManager{}}).RegisterRoutes(failing)
		response := usageRequest(t, failing, "sk-bf-one", "/api/governance/virtual-keys/usage")
		require.Equal(t, fasthttp.StatusInternalServerError, response.Response.StatusCode())
		require.False(t, strings.Contains(string(response.Response.Body()), "private database detail"))
	})
}
