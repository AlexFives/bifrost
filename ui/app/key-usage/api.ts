import { getApiBaseUrl } from "@/lib/utils/port";
import { z } from "zod";

const budgetSchema = z.object({
	id: z.string(),
	max_limit: z.number(),
	current_usage: z.number(),
	reset_duration: z.string(),
	last_reset: z.string().optional(),
	override_amount: z.number().optional(),
	override_mode: z.enum(["cycles", "forever"]).optional(),
	override_cycles_remaining: z.number().optional(),
});

const rateLimitSchema = z.object({
	id: z.string(),
	token_max_limit: z.number().nullish(),
	token_current_usage: z.number().default(0),
	token_reset_duration: z.string().nullish(),
	request_max_limit: z.number().nullish(),
	request_current_usage: z.number().default(0),
	request_reset_duration: z.string().nullish(),
});

const budgetList = z
	.array(budgetSchema)
	.nullish()
	.transform((value) => value ?? []);
const quotaSchema = z.object({
	virtual_key_name: z.string(),
	is_active: z.boolean(),
	budgets: budgetList,
	rate_limit: rateLimitSchema.nullish(),
	rate_limits: z
		.array(rateLimitSchema)
		.nullish()
		.transform((value) => value ?? []),
	provider_configs: z
		.array(
			z.object({
				provider: z.string(),
				budgets: budgetList,
				rate_limit: rateLimitSchema.nullish(),
			}),
		)
		.nullish()
		.transform((value) => value ?? []),
	model_configs: z
		.array(
			z.object({
				model_name: z.string(),
				provider: z.string().nullish(),
				budgets: budgetList,
				rate_limit: rateLimitSchema.nullish(),
			}),
		)
		.nullish()
		.transform((value) => value ?? []),
});

const usageSchema = z.object({
	period: z.enum(["24h", "7d", "30d"]),
	logging_enabled: z.boolean(),
	stats: z
		.object({
			total_requests: z.number(),
			total_tokens: z.number(),
			prompt_tokens: z.number(),
			completion_tokens: z.number(),
			total_cost: z.number(),
		})
		.nullable(),
	models: z.array(
		z.object({
			model: z.string(),
			provider: z.string().default(""),
			total_requests: z.number(),
			total_tokens: z.number(),
			total_cost: z.number(),
		}),
	),
});

export type Budget = z.infer<typeof budgetSchema>;
export type RateLimit = z.infer<typeof rateLimitSchema>;
export type Quota = z.infer<typeof quotaSchema>;
export type Usage = z.infer<typeof usageSchema>;
export type Period = Usage["period"];

export class KeyUsageError extends Error {
	constructor(readonly status: number) {
		super("Failed to load key usage");
	}
}

async function request<T>(path: string, key: string, schema: z.ZodType<T>, signal: AbortSignal): Promise<T> {
	const controller = new AbortController();
	const abort = () => controller.abort();
	if (signal.aborted) abort();
	else signal.addEventListener("abort", abort, { once: true });
	const timeout = setTimeout(abort, 15_000);
	try {
		const response = await fetch(`${getApiBaseUrl()}${path}`, {
			headers: { "x-bf-vk": key },
			credentials: "omit",
			cache: "no-store",
			signal: controller.signal,
		});
		if (!response.ok) throw new KeyUsageError(response.status);
		const result = schema.safeParse(await response.json());
		if (!result.success) throw new KeyUsageError(0);
		return result.data;
	} finally {
		clearTimeout(timeout);
		signal.removeEventListener("abort", abort);
	}
}

export function loadQuota(key: string, signal: AbortSignal): Promise<Quota> {
	return request("/governance/virtual-keys/quota", key, quotaSchema, signal);
}

export function loadUsage(key: string, period: Period, signal: AbortSignal): Promise<Usage> {
	return request(`/governance/virtual-keys/usage?period=${period}`, key, usageSchema, signal);
}