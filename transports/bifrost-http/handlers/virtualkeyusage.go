package handlers

import (
	"time"

	"github.com/maximhq/bifrost/framework/logstore"
	"github.com/valyala/fasthttp"
)

type keyUsageStats struct {
	TotalRequests    int64   `json:"total_requests"`
	TotalTokens      int64   `json:"total_tokens"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	TotalCost        float64 `json:"total_cost"`
}

// getVirtualKeyUsage returns aggregate log statistics only for the presented virtual key.
func (h *GovernanceHandler) getVirtualKeyUsage(ctx *fasthttp.RequestCtx) {
	ctx.Response.Header.Set("Cache-Control", "private, no-store")
	vk, ok := h.selfServiceVirtualKey(ctx)
	if !ok {
		return
	}

	period := string(ctx.QueryArgs().Peek("period"))
	var duration time.Duration
	switch period {
	case "", "30d":
		period, duration = "30d", 30*24*time.Hour
	case "7d":
		duration = 7 * 24 * time.Hour
	case "24h":
		duration = 24 * time.Hour
	default:
		SendError(ctx, fasthttp.StatusBadRequest, "Invalid period")
		return
	}

	end := time.Now().UTC()
	start := end.Add(-duration)
	response := map[string]interface{}{
		"period":          period,
		"start_time":      start,
		"end_time":        end,
		"logging_enabled": h.logManager != nil,
		"stats":           nil,
		"models":          []quotaModelSpend{},
	}
	if h.logManager == nil {
		SendJSON(ctx, response)
		return
	}

	// Do not accept a key ID from the caller: both queries use the authenticated key's ID.
	limit := 20
	filters := &logstore.SearchFilters{VirtualKeyIDs: []string{vk.ID}, StartTime: &start, EndTime: &end, RankingLimit: &limit}
	stats, err := h.logManager.GetStats(ctx, filters)
	if err != nil || stats == nil {
		SendError(ctx, fasthttp.StatusInternalServerError, "Failed to load usage statistics")
		return
	}
	// Hourly materialized rankings can include the hour before the window and
	// omit recent logs until refresh. Keep this breakdown within exact bounds.
	rankingFilters := *filters
	rankingFilters.RequireExactTimeWindow = true
	rankings, err := h.logManager.GetModelRankings(ctx, &rankingFilters)
	if err != nil {
		SendError(ctx, fasthttp.StatusInternalServerError, "Failed to load model usage")
		return
	}
	models := make([]quotaModelSpend, 0)
	if rankings != nil {
		for _, entry := range rankings.Rankings {
			models = append(models, quotaModelSpend{
				Model: entry.Model, Provider: entry.Provider, TotalRequests: entry.TotalRequests,
				TotalTokens: entry.TotalTokens, TotalCost: entry.TotalCost,
				InputCost: entry.InputCost, OutputCost: entry.OutputCost, AdditionalCost: entry.AdditionalCost,
			})
		}
	}
	response["stats"] = keyUsageStats{
		TotalRequests: stats.TotalRequests, TotalTokens: stats.TotalTokens,
		PromptTokens: stats.PromptTokens, CompletionTokens: stats.CompletionTokens, TotalCost: stats.TotalCost,
	}
	response["models"] = models
	SendJSON(ctx, response)
}
