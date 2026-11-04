import { LanguageSwitcher } from "@/components/language-switcher";
import { ThemeToggle } from "@/components/themeToggle";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { withBasePath } from "@/lib/utils/basePath";
import { Eye, EyeOff, KeyRound } from "lucide-react";
import { useEffect, useRef, useState, type FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { KeyUsageError, loadQuota, loadUsage, type Period, type Quota, type Usage } from "./api";
import { KeyLimits } from "./limits";
import { KeyStatistics } from "./statistics";

export default function KeyUsagePage() {
	const { t } = useTranslation("keyUsage");
	const { t: login } = useTranslation("login");
	const [draft, setDraft] = useState("");
	const [visible, setVisible] = useState(false);
	const [quota, setQuota] = useState<Quota | null>(null);
	const [usage, setUsage] = useState<Usage | null>(null);
	const [period, setPeriod] = useState<Period>("30d");
	const [loading, setLoading] = useState(false);
	const [loginError, setLoginError] = useState<"invalidKey" | "loadFailed" | null>(null);
	const [statsError, setStatsError] = useState(false);
	const keyRef = useRef("");
	const requestRef = useRef<AbortController | null>(null);

	useEffect(
		() => () => {
			requestRef.current?.abort();
			keyRef.current = "";
		},
		[],
	);

	const beginRequest = () => {
		requestRef.current?.abort();
		const controller = new AbortController();
		requestRef.current = controller;
		return controller;
	};

	const signIn = async (event: FormEvent<HTMLFormElement>) => {
		event.preventDefault();
		const key = draft.trim();
		if (!key) return;
		const controller = beginRequest();
		setLoading(true);
		setLoginError(null);
		setStatsError(false);
		try {
			const nextQuota = await loadQuota(key, controller.signal);
			if (controller.signal.aborted) return;
			keyRef.current = key;
			setDraft("");
			setQuota(nextQuota);
			try {
				const nextUsage = await loadUsage(key, period, controller.signal);
				if (!controller.signal.aborted) setUsage(nextUsage);
			} catch {
				if (!controller.signal.aborted) setStatsError(true);
			}
		} catch (error) {
			if (!controller.signal.aborted)
				setLoginError(error instanceof KeyUsageError && (error.status === 401 || error.status === 403) ? "invalidKey" : "loadFailed");
		} finally {
			if (!controller.signal.aborted) setLoading(false);
		}
	};

	const refreshUsage = async (nextPeriod: Period) => {
		const controller = beginRequest();
		setPeriod(nextPeriod);
		setUsage(null);
		setStatsError(false);
		setLoading(true);
		try {
			const nextUsage = await loadUsage(keyRef.current, nextPeriod, controller.signal);
			if (!controller.signal.aborted) setUsage(nextUsage);
		} catch {
			if (!controller.signal.aborted) setStatsError(true);
		} finally {
			if (!controller.signal.aborted) setLoading(false);
		}
	};

	const signOut = () => {
		requestRef.current?.abort();
		keyRef.current = "";
		setDraft("");
		setVisible(false);
		setQuota(null);
		setUsage(null);
		setLoginError(null);
		setStatsError(false);
		setLoading(false);
		setPeriod("30d");
	};

	return (
		<div className="bg-background text-foreground min-h-screen">
			<header className="border-border border-b">
				<div className="mx-auto flex h-16 max-w-6xl items-center justify-between px-5 sm:px-8">
					{/* i18n-ignore: product name */}
					<img src={withBasePath("/bifrost-logo.webp")} alt="Bifrost" className="h-6 w-auto dark:hidden" />
					{/* i18n-ignore: product name */}
					<img src={withBasePath("/bifrost-logo-dark.webp")} alt="Bifrost" className="hidden h-6 w-auto dark:block" />
					<div className="flex items-center gap-1">
						<LanguageSwitcher />
						<ThemeToggle />
					</div>
				</div>
			</header>
			<main className="mx-auto w-full max-w-6xl px-5 py-10 sm:px-8 sm:py-12">
				{!quota ? (
					<div className="mx-auto max-w-md pt-6 sm:pt-14">
						<div className="bg-muted text-foreground mb-5 flex size-11 items-center justify-center rounded-sm">
							<KeyRound className="size-5" />
						</div>
						<h1 className="text-2xl font-semibold tracking-tight">{t("title")}</h1>
						<p className="text-muted-foreground mt-2 text-sm">{t("description")}</p>
						<form className="bg-card mt-8 space-y-5 rounded-sm border p-6" onSubmit={signIn}>
							<div className="space-y-2">
								<Label htmlFor="virtual-key">{t("keyLabel")}</Label>
								<div className="relative">
									<Input
										id="virtual-key"
										type={visible ? "text" : "password"}
										autoComplete="off"
										placeholder={t("keyPlaceholder")}
										value={draft}
										onChange={(event) => setDraft(event.target.value)}
										required
										className="h-10 pr-10"
									/>
									<button
										type="button"
										onClick={() => setVisible(!visible)}
										aria-label={visible ? login("hidePassword") : login("showPassword")}
										className="text-muted-foreground hover:text-foreground absolute inset-y-0 right-3 flex items-center"
									>
										<span className="sr-only">{visible ? login("hidePassword") : login("showPassword")}</span>
										{visible ? <EyeOff className="size-4" /> : <Eye className="size-4" />}
									</button>
								</div>
								<p className="text-muted-foreground text-xs">{t("keyHelp")}</p>
							</div>
							{loginError && (
								<p role="alert" className="text-destructive text-sm">
									{t(loginError)}
								</p>
							)}
							<Button type="submit" size="lg" className="w-full" disabled={loading || !draft.trim()} isLoading={loading}>
								{t("viewUsage")}
							</Button>
						</form>
					</div>
				) : (
					<div className="space-y-10">
						<div className="flex flex-wrap items-start justify-between gap-4">
							<div className="min-w-0">
								<h1 className="text-2xl font-semibold tracking-tight">{t("title")}</h1>
								<p className="text-muted-foreground mt-2 text-sm break-all">{quota.virtual_key_name}</p>
							</div>
							<Button type="button" variant="outline" size="sm" onClick={signOut}>
								{t("switchKey")}
							</Button>
						</div>
						{!quota.is_active && (
							<p role="status" className="bg-muted rounded-sm p-4 text-sm">
								{t("inactive")}
							</p>
						)}
						<KeyStatistics
							usage={usage}
							period={period}
							loading={loading}
							error={statsError}
							onPeriod={(value) => void refreshUsage(value)}
							onRetry={() => void refreshUsage(period)}
						/>
						<KeyLimits quota={quota} />
					</div>
				)}
			</main>
		</div>
	);
}