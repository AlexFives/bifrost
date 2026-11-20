package handlers

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/stretchr/testify/require"
)

type providerBalanceRoundTripper func(*http.Request) (*http.Response, error)

func (roundTrip providerBalanceRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func TestFetchProviderAccountBalances(t *testing.T) {
	t.Run("OpenRouter derives the remaining credits from its account totals", func(t *testing.T) {
		client := &http.Client{Transport: providerBalanceRoundTripper(func(request *http.Request) (*http.Response, error) {
			require.Equal(t, "https://openrouter.example/api/v1/credits", request.URL.String())
			require.Equal(t, "Bearer management-secret", request.Header.Get("Authorization"))
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"data":{"total_credits":100.5,"total_usage":25.75}}`)),
				Header:     make(http.Header),
			}, nil
		})}

		balances, err := fetchProviderAccountBalances(context.Background(), schemas.OpenRouter, "https://openrouter.example/api", "management-secret", client)
		require.NoError(t, err)
		require.Equal(t, []providerBalanceAmount{{Currency: "USD", Balance: 74.75}}, balances)
	})

	t.Run("DeepSeek returns each reported currency separately", func(t *testing.T) {
		client := &http.Client{Transport: providerBalanceRoundTripper(func(request *http.Request) (*http.Response, error) {
			require.Equal(t, "https://deepseek.example/user/balance", request.URL.String())
			require.Equal(t, "Bearer deepseek-secret", request.Header.Get("Authorization"))
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(`{"is_available":true,"balance_infos":[` +
					`{"currency":"USD","total_balance":"14.25"},` +
					`{"currency":"CNY","total_balance":"80.00"}]}`)),
				Header: make(http.Header),
			}, nil
		})}

		balances, err := fetchProviderAccountBalances(context.Background(), schemas.DeepSeek, "https://deepseek.example", "deepseek-secret", client)
		require.NoError(t, err)
		require.Equal(t, []providerBalanceAmount{{Currency: "USD", Balance: 14.25}, {Currency: "CNY", Balance: 80}}, balances)
	})

	t.Run("provider errors do not leak upstream response bodies", func(t *testing.T) {
		client := &http.Client{Transport: providerBalanceRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Body:       io.NopCloser(strings.NewReader("sensitive-provider-response")),
				Header:     make(http.Header),
			}, nil
		})}

		_, err := fetchProviderAccountBalances(context.Background(), schemas.OpenRouter, "https://openrouter.example/api", "management-secret", client)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "sensitive-provider-response")
		require.NotContains(t, err.Error(), "management-secret")
	})
}
