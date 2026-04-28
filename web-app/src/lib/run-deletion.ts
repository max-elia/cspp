import { apiFetch } from '$lib/api';
import type { WebAppCatalog } from '$lib/types';

export type DeleteMode = 'run' | 'instance';

export async function deleteCatalogItem(options: {
	targetId: string;
	mode: DeleteMode;
}): Promise<{ nextCatalog: WebAppCatalog | null; nextInstanceId: string | null; nextRunId: string | null }> {
	const { targetId, mode } = options;
	const response = await apiFetch(
		mode === 'instance' ? `/api/instances/${encodeURIComponent(targetId)}` : `/api/runs/${encodeURIComponent(targetId)}`,
		{ method: 'DELETE' }
	);
	const text = await response.text();
	let payload: Record<string, unknown> | null = null;
	try {
		payload = text ? (JSON.parse(text) as Record<string, unknown>) : null;
	} catch {
		payload = null;
	}
	if (!response.ok) {
		const detail = typeof payload?.detail === 'string' ? payload.detail : text || `Delete failed: ${response.status}`;
		throw new Error(detail);
	}
	const nextCatalog = (payload?.catalog as WebAppCatalog | undefined) ?? null;
	const nextInstanceId = nextCatalog?.latest_instance_id ?? nextCatalog?.instances?.[0]?.instance_id ?? null;
	const nextRunId =
		nextCatalog?.latest_run_id ??
		nextCatalog?.instances?.find((instance) => instance.instance_id === nextInstanceId)?.latest_run_id ??
		null;
	return { nextCatalog, nextInstanceId, nextRunId };
}
