import { createFileRoute, Outlet, useChildMatches } from "@tanstack/react-router";
import { NoPermissionView } from "@/components/noPermissionView";
import { RbacOperation, RbacResource, useRbac } from "@enterprise/lib";
import WorkloadIdentityPage from "./page";

function RouteComponent() {
	const hasAccess = useRbac(RbacResource.WorkloadIdentity, RbacOperation.View);
	const childMatches = useChildMatches();
	if (!hasAccess) {
		return <NoPermissionView entity="workload identity" />;
	}
	return childMatches.length === 0 ? <WorkloadIdentityPage /> : <Outlet />;
}

export const Route = createFileRoute("/workspace/workload-identity")({
	component: RouteComponent,
});