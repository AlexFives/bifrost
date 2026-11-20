package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	configstoreTables "github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/valyala/fasthttp"
)

const maxConcurrentProviderBalanceLookups = 8

var providerBalanceHTTPClient = &http.Client{Timeout: 4 * time.Second}

type providerAccountBalance struct {
	Provider     string   `json:"provider"`
	KeyName      string   `json:"key_name,omitempty"`
	Currency     string   `json:"currency,omitempty"`
	Balance      *float64 `json:"balance,omitempty"`
	Status       string   `json:"status"`
	CanAggregate bool     `json:"can_aggregate"`
}

type providerBalanceAmount struct {
	Currency string
	Balance  float64
}

type providerBalanceTarget struct {
	provider schemas.ModelProvider
	baseURL  string
	keyName  string
	secret   string
}

func virtualKeyAllowsProviderBalance(vk *configstoreTables.TableVirtualKey, provider schemas.ModelProvider, keyID string) bool {
	for i := range vk.ProviderConfigs {
		providerConfig := &vk.ProviderConfigs[i]
		if providerConfig.Provider != string(provider) {
			continue
		}
		if providerConfig.AllowAllKeys {
			return true
		}
		for _, key := range providerConfig.Keys {
			if key.KeyID == keyID {
				return true
			}
		}
		return false
	}
	return vk.AllowAllProviders
}

func (h *GovernanceHandler) providerAccountBalances(ctx *fasthttp.RequestCtx, vk *configstoreTables.TableVirtualKey) []providerAccountBalance {
	balances := make([]providerAccountBalance, 0)
	providers := []schemas.ModelProvider{schemas.DeepSeek, schemas.OpenRouter}
	targets := make([]providerBalanceTarget, 0)
	providerFailures := make(map[schemas.ModelProvider]bool)

	for _, provider := range providers {
		providerAllowed := vk.AllowAllProviders
		for i := range vk.ProviderConfigs {
			if vk.ProviderConfigs[i].Provider == string(provider) {
				providerAllowed = true
				break
			}
		}
		if !providerAllowed {
			continue
		}

		providerConfig, err := h.configStore.GetProviderConfig(ctx, provider)
		if err != nil {
			if !errors.Is(err, configstore.ErrNotFound) {
				balances = append(balances, providerAccountBalance{Provider: string(provider), Status: "unavailable"})
				providerFailures[provider] = true
			}
			continue
		}
		if providerConfig == nil {
			continue
		}
		baseURL := ""
		if providerConfig.NetworkConfig != nil {
			baseURL = providerConfig.NetworkConfig.BaseURL
		}
		for _, key := range providerConfig.Keys {
			if key.Enabled != nil && !*key.Enabled {
				continue
			}
			secret := key.Value.GetValue()
			if secret == "" || !virtualKeyAllowsProviderBalance(vk, provider, key.ID) {
				continue
			}
			targets = append(targets, providerBalanceTarget{provider: provider, baseURL: baseURL, keyName: key.Name, secret: secret})
		}
	}
	providerKeyCounts := make(map[schemas.ModelProvider]int)
	for _, target := range targets {
		providerKeyCounts[target.provider]++
	}

	var balanceMu sync.Mutex
	var wait sync.WaitGroup
	semaphore := make(chan struct{}, maxConcurrentProviderBalanceLookups)
	for _, target := range targets {
		wait.Add(1)
		go func(target providerBalanceTarget) {
			defer wait.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				balanceMu.Lock()
				providerFailures[target.provider] = true
				balances = append(balances, providerAccountBalance{Provider: string(target.provider), KeyName: target.keyName, Status: "unavailable"})
				balanceMu.Unlock()
				return
			}

			amounts, err := fetchProviderAccountBalances(ctx, target.provider, target.baseURL, target.secret, providerBalanceHTTPClient)
			balanceMu.Lock()
			defer balanceMu.Unlock()
			if err != nil || len(amounts) == 0 {
				providerFailures[target.provider] = true
				balances = append(balances, providerAccountBalance{Provider: string(target.provider), KeyName: target.keyName, Status: "unavailable"})
				return
			}
			for _, amount := range amounts {
				balance := amount.Balance
				balances = append(balances, providerAccountBalance{
					Provider: string(target.provider), KeyName: target.keyName,
					Currency: amount.Currency, Balance: &balance, Status: "available",
				})
			}
		}(target)
	}
	wait.Wait()
	for i := range balances {
		provider := schemas.ModelProvider(balances[i].Provider)
		balances[i].CanAggregate = balances[i].Status == "available" && providerKeyCounts[provider] == 1 && !providerFailures[provider]
	}

	sort.Slice(balances, func(i, j int) bool {
		if balances[i].Provider != balances[j].Provider {
			return balances[i].Provider < balances[j].Provider
		}
		if balances[i].KeyName != balances[j].KeyName {
			return balances[i].KeyName < balances[j].KeyName
		}
		return balances[i].Currency < balances[j].Currency
	})
	return balances
}

func fetchProviderAccountBalances(ctx context.Context, provider schemas.ModelProvider, baseURL, apiKey string, client *http.Client) ([]providerBalanceAmount, error) {
	endpointPath := ""
	if baseURL == "" {
		switch provider {
		case schemas.DeepSeek:
			baseURL = "https://api.deepseek.com"
		case schemas.OpenRouter:
			baseURL = "https://openrouter.ai/api"
		}
	}
	switch provider {
	case schemas.DeepSeek:
		endpointPath = "/user/balance"
	case schemas.OpenRouter:
		endpointPath = "/v1/credits"
	default:
		return nil, fmt.Errorf("provider %q does not expose a supported balance API", provider)
	}

	parsedBaseURL, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil || parsedBaseURL.Host == "" || (parsedBaseURL.Scheme != "http" && parsedBaseURL.Scheme != "https") {
		return nil, errors.New("invalid provider base URL")
	}
	if parsedBaseURL.User != nil || parsedBaseURL.RawQuery != "" || parsedBaseURL.Fragment != "" {
		return nil, errors.New("invalid provider base URL")
	}
	parsedBaseURL.Path = strings.TrimRight(parsedBaseURL.Path, "/") + endpointPath
	parsedBaseURL.RawPath = ""
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsedBaseURL.String(), nil)
	if err != nil {
		return nil, errors.New("failed to create provider balance request")
	}
	request.Header.Set("Authorization", "Bearer "+apiKey)
	if client == nil {
		client = providerBalanceHTTPClient
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, errors.New("provider balance request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errors.New("provider balance request returned a non-200 status")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, errors.New("failed to read provider balance response")
	}

	switch provider {
	case schemas.DeepSeek:
		var result struct {
			BalanceInfos []struct {
				Currency     string `json:"currency"`
				TotalBalance string `json:"total_balance"`
			} `json:"balance_infos"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, errors.New("failed to decode provider balance response")
		}
		amounts := make([]providerBalanceAmount, 0, len(result.BalanceInfos))
		for _, info := range result.BalanceInfos {
			currency := strings.ToUpper(info.Currency)
			if currency != "USD" && currency != "CNY" {
				return nil, errors.New("provider returned an unsupported balance currency")
			}
			balance, err := strconv.ParseFloat(info.TotalBalance, 64)
			if err != nil || math.IsNaN(balance) || math.IsInf(balance, 0) {
				return nil, errors.New("provider returned an invalid balance")
			}
			amounts = append(amounts, providerBalanceAmount{Currency: currency, Balance: balance})
		}
		return amounts, nil
	case schemas.OpenRouter:
		var result struct {
			Data struct {
				TotalCredits *float64 `json:"total_credits"`
				TotalUsage   *float64 `json:"total_usage"`
			} `json:"data"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, errors.New("failed to decode provider balance response")
		}
		if result.Data.TotalCredits == nil || result.Data.TotalUsage == nil {
			return nil, errors.New("provider balance response is missing account totals")
		}
		balance := *result.Data.TotalCredits - *result.Data.TotalUsage
		return []providerBalanceAmount{{Currency: "USD", Balance: balance}}, nil
	default:
		return nil, fmt.Errorf("provider %q does not expose a supported balance API", provider)
	}
}
