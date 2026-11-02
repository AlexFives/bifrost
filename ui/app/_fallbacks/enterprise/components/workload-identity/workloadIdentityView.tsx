import { Fingerprint } from "lucide-react";
import { useTranslation } from "react-i18next";
import ContactUsView from "../views/contactUsView";

export default function WorkloadIdentityView() {
	const { t } = useTranslation("shell");

	return (
		<div className="h-full w-full">
			<ContactUsView
				className="mx-auto min-h-[80vh]"
				icon={<Fingerprint className="h-[5.5rem] w-[5.5rem]" strokeWidth={1} />}
				title={t("workloadIdentity.unlockTitle")}
				description={t("workloadIdentity.unlockDescription")}
				readmeLink="https://docs.getbifrost.ai/enterprise/workload-identity/overview"
			/>
		</div>
	);
}