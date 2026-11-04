import { ThemeProvider } from "@/components/themeProvider";
import i18n from "@/lib/i18n";
import { createFileRoute } from "@tanstack/react-router";
import { I18nextProvider } from "react-i18next";
import KeyUsagePage from "./page";

export const Route = createFileRoute("/key-usage")({ component: KeyUsageLayout });

function KeyUsageLayout() {
	return (
		<I18nextProvider i18n={i18n}>
			<ThemeProvider attribute="class" defaultTheme="system" enableSystem>
				<KeyUsagePage />
			</ThemeProvider>
		</I18nextProvider>
	);
}