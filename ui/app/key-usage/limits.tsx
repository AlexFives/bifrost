import { Progress } from "@/components/ui/progress";
import { getLocale } from "@/lib/i18n";
import { getEffectiveBudgetLimit, parseResetPeriod } from "@/lib/utils/governance";
import { useTranslation } from "react-i18next";
import type { Budget, Quota, RateLimit } from "./api";

function formatMoney(value: number): string {
	return new Intl.NumberFormat(getLocale(), {
		style: "currency",
		currency: "USD",
		minimumFractionDigits: 2,
		maximumFractionDigits: 6,
	}).format(value);
}

function LimitRow({
	label,
	used,
	maximum,
	period,
	money,
}: {
	label: string;
	used: number;
	maximum: number;
	period?: string | null;
	money?: boolean;
}) {
	const { t } = useTranslation("keyUsage");
	const format = (value: number) => (money ? formatMoney(value) : new Intl.NumberFormat(getLocale()).format(value));
	const percent = maximum > 0 ? Math.min(100, Math.max(0, (used / maximum) * 100)) : 100;
	return (
		<div className="space-y-2 py-3">
			<div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1 text-sm">
				<span className="font-medium">{label}</span>
				<span className="text-muted-foreground tabular-nums">
					{format(used)} / {format(maximum)}
				</span>
			</div>
			<Progress value={percent} aria-label={label} />
			<div className="text-muted-foreground flex flex-wrap justify-between gap-x-4 gap-y-1 text-xs">
				<span>
					{t("remaining")}: {format(Math.max(0, maximum - used))}
				</span>
				{period && <span>{t("resets", { period: parseResetPeriod(period) })}</span>}
			</div>
		</div>
	);
}

function BudgetRows({ budgets }: { budgets: Budget[] }) {
	const { t } = useTranslation("keyUsage");
	return budgets.map((budget) => (
		<LimitRow
			key={budget.id}
			label={t("spent")}
			used={budget.current_usage}
			maximum={getEffectiveBudgetLimit(budget)}
			period={budget.reset_duration}
			money
		/>
	));
}

function RateRows({ limit }: { limit: RateLimit | null | undefined }) {
	const { t } = useTranslation("observability");
	if (!limit) return null;
	return (
		<>
			{limit.token_max_limit != null && (
				<LimitRow
					label={t("labels.tokens")}
					used={limit.token_current_usage}
					maximum={limit.token_max_limit}
					period={limit.token_reset_duration}
				/>
			)}
			{limit.request_max_limit != null && (
				<LimitRow
					label={t("labels.requests")}
					used={limit.request_current_usage}
					maximum={limit.request_max_limit}
					period={limit.request_reset_duration}
				/>
			)}
		</>
	);
}

function hasRateLimit(limit: RateLimit | null | undefined): boolean {
	return !!limit && (limit.token_max_limit != null || limit.request_max_limit != null);
}

export function KeyLimits({ quota }: { quota: Quota }) {
	const { t } = useTranslation("keyUsage");
	const overallRates = quota.rate_limits.length ? quota.rate_limits : quota.rate_limit ? [quota.rate_limit] : [];
	const providers = quota.provider_configs.filter((provider) => provider.budgets.length > 0 || hasRateLimit(provider.rate_limit));
	const models = quota.model_configs.filter((model) => model.budgets.length > 0 || hasRateLimit(model.rate_limit));
	const hasLimits = quota.budgets.length > 0 || overallRates.some(hasRateLimit) || providers.length > 0 || models.length > 0;
	return (
		<section aria-labelledby="limits-heading" className="space-y-5">
			<div>
				<h2 id="limits-heading" className="text-lg font-semibold">
					{t("limitsTitle")}
				</h2>
				<p className="text-muted-foreground mt-1 text-sm">{t("limitsDescription")}</p>
			</div>
			{!hasLimits && <p className="text-muted-foreground rounded-sm border p-5 text-sm">{t("noLimits")}</p>}
			{(quota.budgets.length > 0 || overallRates.some(hasRateLimit)) && (
				<div className="bg-card rounded-sm border p-5">
					<h3 className="font-medium">{t("overallLimits")}</h3>
					<BudgetRows budgets={quota.budgets} />
					{overallRates.map((rate) => (
						<RateRows key={rate.id} limit={rate} />
					))}
				</div>
			)}
			{providers.length > 0 && (
				<div className="bg-card rounded-sm border p-5">
					<h3 className="font-medium">{t("providerLimits")}</h3>
					<div className="divide-border divide-y">
						{providers.map((provider) => (
							<div key={provider.provider} className="py-3 first:pt-4 last:pb-0">
								<p className="text-sm font-medium break-all">{provider.provider}</p>
								<BudgetRows budgets={provider.budgets} />
								<RateRows limit={provider.rate_limit} />
							</div>
						))}
					</div>
				</div>
			)}
			{models.length > 0 && (
				<div className="bg-card rounded-sm border p-5">
					<h3 className="font-medium">{t("modelLimits")}</h3>
					<div className="divide-border divide-y">
						{models.map((model) => (
							<div key={`${model.provider ?? ""}/${model.model_name}`} className="py-3 first:pt-4 last:pb-0">
								<p className="text-sm font-medium break-all">
									{model.provider ? `${model.provider}/${model.model_name}` : model.model_name}
								</p>
								<BudgetRows budgets={model.budgets} />
								<RateRows limit={model.rate_limit} />
							</div>
						))}
					</div>
				</div>
			)}
		</section>
	);
}