import { Button } from "@/components/ui/button";
import { getLocale } from "@/lib/i18n";
import { useTranslation } from "react-i18next";
import type { Period, Usage } from "./api";

const periods: { value: Period; label: "hours24" | "days7" | "days30" }[] = [
	{ value: "24h", label: "hours24" },
	{ value: "7d", label: "days7" },
	{ value: "30d", label: "days30" },
];

export function KeyStatistics({
	usage,
	period,
	loading,
	error,
	onPeriod,
	onRetry,
}: {
	usage: Usage | null;
	period: Period;
	loading: boolean;
	error: boolean;
	onPeriod: (period: Period) => void;
	onRetry: () => void;
}) {
	const { t } = useTranslation("keyUsage");
	const { t: labels } = useTranslation("observability");
	const { t: common } = useTranslation("common");
	const number = (value: number) => new Intl.NumberFormat(getLocale()).format(value);
	const money = (value: number) =>
		new Intl.NumberFormat(getLocale(), { style: "currency", currency: "USD", minimumFractionDigits: 2, maximumFractionDigits: 6 }).format(
			value,
		);
	return (
		<section aria-labelledby="usage-heading" className="space-y-5">
			<div className="flex flex-wrap items-start justify-between gap-4">
				<div>
					<h2 id="usage-heading" className="text-lg font-semibold">
						{t("usageTitle")}
					</h2>
					<p className="text-muted-foreground mt-1 text-sm">{t("usageDescription")}</p>
				</div>
				<div role="group" aria-label={t("periodLabel")} className="bg-muted flex flex-wrap gap-1 rounded-sm p-1">
					{periods.map((item) => (
						<Button
							key={item.value}
							type="button"
							size="sm"
							variant={period === item.value ? "secondary" : "ghost"}
							aria-pressed={period === item.value}
							disabled={loading}
							onClick={() => onPeriod(item.value)}
						>
							{t(item.label)}
						</Button>
					))}
				</div>
			</div>
			{loading && (
				<p className="text-muted-foreground text-sm" role="status">
					{common("loading")}
				</p>
			)}
			{!loading && error && (
				<div role="alert" className="bg-card rounded-sm border p-5 text-sm">
					<p>{t("statsError")}</p>
					<Button variant="outline" size="sm" className="mt-3" onClick={onRetry}>
						{t("statsRetry")}
					</Button>
				</div>
			)}
			{!loading && !error && usage && !usage.logging_enabled && (
				<p className="text-muted-foreground rounded-sm border p-5 text-sm">{t("loggingDisabled")}</p>
			)}
			{!loading && !error && usage?.logging_enabled && usage.stats && (
				<>
					<div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
						{[
							{ label: labels("labels.requests"), value: number(usage.stats.total_requests) },
							{ label: labels("labels.tokens"), value: number(usage.stats.total_tokens) },
							{ label: labels("labels.cost"), value: money(usage.stats.total_cost) },
						].map((item) => (
							<div key={item.label} className="bg-card rounded-sm border p-5">
								<p className="text-muted-foreground text-sm">{item.label}</p>
								<p className="mt-2 text-2xl font-semibold tabular-nums">{item.value}</p>
							</div>
						))}
					</div>
					<p className="text-muted-foreground text-xs">{t("priceDisclaimer")}</p>
					<div className="bg-card rounded-sm border">
						<h3 className="border-b px-5 py-4 font-medium">{t("modelUsage")}</h3>
						{usage.models.length === 0 ? (
							<p className="text-muted-foreground p-5 text-sm">{t("noModels")}</p>
						) : (
							<div className="divide-border divide-y">
								{usage.models.map((model) => (
									<div
										key={`${model.provider}/${model.model}`}
										className="flex flex-col items-start justify-between gap-3 px-5 py-4 text-sm sm:flex-row sm:items-center"
									>
										<div className="min-w-0">
											<p className="font-medium break-all">{model.model}</p>
											<p className="text-muted-foreground mt-1 text-xs break-all">{model.provider}</p>
										</div>
										<div className="text-muted-foreground tabular-nums sm:text-right">
											<p>{money(model.total_cost)}</p>
											<p className="mt-1 text-xs">
												{t("modelTotals", { requests: number(model.total_requests), tokens: number(model.total_tokens) })}
											</p>
										</div>
									</div>
								))}
							</div>
						)}
					</div>
				</>
			)}
		</section>
	);
}