import { get, writable } from 'svelte/store';
import { getMeta, getMirroredJson, getMirroredText } from '$lib/file-mirror-db';
import type {
	AppState,
	FeatureCollection,
	FrontendManifest,
	InstanceCatalogEntry,
	MapDemandRow,
	OverviewPayload,
	PipelineProgress,
	RunIndexEntry,
	RunsIndex,
	WebAppCatalog
} from '$lib/types';

const POLL_INTERVAL_MS = 1000;
const CATALOG_PATH = 'exports/state/web_app_catalog.json';

const initialState: AppState = {
	catalog: null,
	runsIndex: null,
	selectedInstanceId: null,
	selectedRunId: null,
	instanceManifest: null,
	instanceOverview: null,
	manifest: null,
	overview: null,
	pipelineProgress: null,
	instancePayload: null,
	runConfig: null,
	mapSummary: null,
	mapData: null,
	mapDemandRows: [],
	stage1: null,
	stage2: null,
	stage3: null,
	activity: null,
	alerts: [],
	details: {
		stage1Clusters: {},
		stage2Scenarios: {},
		stage3Clusters: {},
		stage3Scopes: {}
	},
	polling: {
		intervalMs: POLL_INTERVAL_MS,
		inFlight: false,
		lastStartedAt: null,
		lastCompletedAt: null,
		lastSuccessfulAt: null,
		consecutiveFailures: 0,
		fileCount: 0,
		mode: 'indexeddb'
	},
	errors: []
};

const state = writable<AppState>(initialState);

let refreshHandle: ReturnType<typeof setInterval> | null = null;
let syncWorker: Worker | null = null;
const activeDetails = new Set<string>();
const activeResources = new Set<string>();

function instanceBasePath(instanceId: string): string {
	return `exports/instances/${instanceId}/frontend`;
}

function runBasePath(runId: string): string {
	return `exports/runs/${runId}/frontend`;
}

function flattenRuns(catalog: WebAppCatalog | null): RunIndexEntry[] {
	const rows: RunIndexEntry[] = [];
	for (const instance of catalog?.instances ?? []) {
		for (const run of instance.runs ?? []) {
			rows.push({ ...run, instance_id: run.instance_id ?? instance.instance_id });
		}
	}
	return rows.sort((a, b) => String(b.run_last_modified_at ?? '').localeCompare(String(a.run_last_modified_at ?? '')));
}

function catalogToRunsIndex(catalog: WebAppCatalog | null): RunsIndex | null {
	if (!catalog) return null;
	return {
		schema_version: catalog.schema_version,
		updated_at: catalog.updated_at,
		latest_run_id: catalog.latest_run_id,
		runs: flattenRuns(catalog)
	};
}

function findInstance(catalog: WebAppCatalog | null, instanceId: string | null): InstanceCatalogEntry | null {
	if (!catalog || !instanceId) return null;
	return (catalog.instances ?? []).find((row) => row.instance_id === instanceId) ?? null;
}

function findRun(catalog: WebAppCatalog | null, runId: string | null): RunIndexEntry | null {
	if (!catalog || !runId) return null;
	for (const instance of catalog.instances ?? []) {
		const match = (instance.runs ?? []).find((row) => row.run_id === runId);
		if (match) return match;
	}
	return null;
}

function parseDemandRows(csvText: string | null): MapDemandRow[] {
	if (!csvText) return [];
	const lines = csvText
		.split(/\r?\n/)
		.map((line) => line.trim())
		.filter(Boolean);
	if (lines.length < 2) return [];
	const header = lines[0].split(',');
	const deliveryDateIndex = header.indexOf('delivery_date');
	const clientNumIndex = header.indexOf('client_num');
	const customerIdIndex = header.indexOf('customer_id');
	const demandIndex = header.indexOf('demand_kg');
	if (deliveryDateIndex < 0 || clientNumIndex < 0 || demandIndex < 0) return [];

	const rows: MapDemandRow[] = [];
	for (const line of lines.slice(1)) {
		const columns = line.split(',');
		const clientNum = Number(columns[clientNumIndex]);
		const demandKg = Number(columns[demandIndex]);
		if (!Number.isFinite(clientNum) || !Number.isFinite(demandKg)) continue;
		rows.push({
			delivery_date: columns[deliveryDateIndex] ?? '',
			client_num: clientNum,
			customer_id: customerIdIndex >= 0 ? (columns[customerIdIndex] ?? null) : null,
			demand_kg: demandKg
		});
	}
	return rows;
}

function parseDemandRowsPayload(payload: Record<string, unknown> | null): MapDemandRow[] {
	if (!payload || !Array.isArray(payload.demand_rows)) return [];
	const rows: MapDemandRow[] = [];
	for (const row of payload.demand_rows) {
		if (!row || typeof row !== 'object') continue;
		const candidate = row as Record<string, unknown>;
		const client_num = Number(candidate.client_num);
		const demand_kg = Number(candidate.demand_kg);
		if (!Number.isFinite(client_num) || !Number.isFinite(demand_kg)) continue;
		rows.push({
			delivery_date: String(candidate.delivery_date ?? ''),
			client_num,
			customer_id: typeof candidate.customer_id === 'string' ? candidate.customer_id : null,
			demand_kg
		});
	}
	return rows;
}

async function loadDetail(path: string): Promise<Record<string, unknown> | null> {
	return await getMirroredJson<Record<string, unknown>>(path);
}

async function loadMapGeojson(basePath: string): Promise<FeatureCollection | null> {
	return (
		(await getMirroredJson<FeatureCollection>(`${basePath}/map/customers.geojson`)) ??
		(await getMirroredJson<FeatureCollection>(`${basePath}/map/customers.json`))
	);
}

async function refreshFromMirror(): Promise<void> {
	const current = get(state);
	if (current.polling.inFlight) return;
	state.update((draft) => ({
		...draft,
		polling: { ...draft.polling, inFlight: true, lastStartedAt: new Date().toISOString() }
	}));

	try {
		const catalog = await getMirroredJson<WebAppCatalog>(CATALOG_PATH);
		const syncStatus = (await getMeta<{ lastSuccessfulAt?: string | null; fileCount?: number; error?: string | null }>('sync-status')) ?? null;
		const currentSelectedInstanceId = get(state).selectedInstanceId;
		const currentSelectedRunId = get(state).selectedRunId;
		const selectedRun = findRun(catalog, currentSelectedRunId);
		let selectedInstanceId = currentSelectedInstanceId;
		if (!selectedInstanceId && selectedRun?.instance_id) {
			selectedInstanceId = selectedRun.instance_id;
		}
		if (!findInstance(catalog, selectedInstanceId)) {
			selectedInstanceId = currentSelectedRunId ? findRun(catalog, currentSelectedRunId)?.instance_id ?? null : null;
		}
		if (!findInstance(catalog, selectedInstanceId)) {
			selectedInstanceId = catalog?.latest_instance_id ?? catalog?.instances?.[0]?.instance_id ?? null;
		}
		const selectedInstance = findInstance(catalog, selectedInstanceId);
		let selectedRunId = currentSelectedRunId;
		if (!findRun(catalog, selectedRunId) || findRun(catalog, selectedRunId)?.instance_id !== selectedInstanceId) {
			selectedRunId = selectedInstance?.latest_run_id ?? selectedInstance?.runs?.[0]?.run_id ?? null;
		}

		const runsIndex = catalogToRunsIndex(catalog);

		if (!selectedInstanceId && !selectedRunId) {
			state.update((draft) => ({
				...draft,
				catalog,
				runsIndex,
				selectedInstanceId: null,
				selectedRunId: null,
				instanceManifest: null,
				instanceOverview: null,
				manifest: null,
				overview: null,
				pipelineProgress: null,
				polling: {
					...draft.polling,
					inFlight: false,
					lastCompletedAt: new Date().toISOString(),
					lastSuccessfulAt: syncStatus?.lastSuccessfulAt ?? draft.polling.lastSuccessfulAt,
					fileCount: syncStatus?.fileCount ?? draft.polling.fileCount
				}
			}));
			return;
		}

		const instanceBase = selectedInstanceId ? instanceBasePath(selectedInstanceId) : null;
		const runBase = selectedRunId ? runBasePath(selectedRunId) : null;

		const [instanceManifest, instanceOverview, instancePayload] = instanceBase
			? await Promise.all([
					getMirroredJson<FrontendManifest>(`${instanceBase}/manifest.json`),
					getMirroredJson<OverviewPayload>(`${instanceBase}/overview.json`),
					getMirroredJson<Record<string, unknown>>(`exports/instances/${selectedInstanceId}/prep/instance/payload.json`)
			  ])
			: [null, null, null];

		const runCore = runBase
			? await Promise.all([
					getMirroredJson<FrontendManifest>(`${runBase}/manifest.json`),
					getMirroredJson<OverviewPayload>(`${runBase}/overview.json`),
					getMirroredJson<PipelineProgress>(`${runBase}/pipeline/progress.json`),
					getMirroredJson<Record<string, unknown>>(`exports/runs/${selectedRunId}/run_config.json`),
					getMirroredJson<Record<string, unknown>>(`${runBase}/map/customers_summary.json`),
					getMirroredJson<Record<string, unknown>>(`${runBase}/stage_1/clusters.json`),
					getMirroredJson<Record<string, unknown>>(`${runBase}/stage_2/scenarios.json`),
					getMirroredJson<Record<string, unknown>>(`${runBase}/stage_3/overview.json`),
					getMirroredJson<Record<string, unknown>>(`${runBase}/activity/recent_events.json`),
					getMirroredJson<{ alerts?: AppState['alerts'] }>(`${runBase}/activity/alerts.json`)
			  ])
			: [null, null, null, null, null, null, null, null, null, null];

		const detailUpdates: Partial<AppState['details']> = {};
		if (runBase) {
			for (const key of activeDetails) {
				const [kind, entityId] = key.split(':', 2);
				let path = '';
				if (kind === 'stage1') path = `${runBase}/stage_1/clusters/${entityId}.json`;
				if (kind === 'stage2') path = `${runBase}/stage_2/scenarios/${entityId}.json`;
				if (kind === 'stage3cluster') path = `${runBase}/stage_3/clusters/${entityId}.json`;
				if (kind === 'stage3scope') path = `${runBase}/stage_3/scopes/${entityId}.json`;
				if (!path) continue;
				const payload = await loadDetail(path);
				if (!payload) continue;
				if (kind === 'stage1') detailUpdates.stage1Clusters = { ...(detailUpdates.stage1Clusters ?? current.details.stage1Clusters), [entityId]: payload };
				if (kind === 'stage2') detailUpdates.stage2Scenarios = { ...(detailUpdates.stage2Scenarios ?? current.details.stage2Scenarios), [entityId]: payload };
				if (kind === 'stage3cluster') detailUpdates.stage3Clusters = { ...(detailUpdates.stage3Clusters ?? current.details.stage3Clusters), [entityId]: payload };
				if (kind === 'stage3scope') detailUpdates.stage3Scopes = { ...(detailUpdates.stage3Scopes ?? current.details.stage3Scopes), [entityId]: payload };
			}
		}

		let mapData: FeatureCollection | null = current.mapData;
		let mapDemandRows = current.mapDemandRows;
		let mapSummary: Record<string, unknown> | null = (runCore[4] as Record<string, unknown> | null) ?? ((instanceOverview?.map_summary as Record<string, unknown> | null) ?? null);
		if (activeResources.has('map')) {
			const [geojson, demandCsv] = runBase
				? await Promise.all([
						loadMapGeojson(runBase),
						getMirroredText(`exports/runs/${selectedRunId}/prep/instance/demand_long.csv`)
				  ])
				: instanceBase
					? await Promise.all([
							loadMapGeojson(instanceBase),
							getMirroredText(`exports/instances/${selectedInstanceId}/prep/instance/demand_long.csv`)
					  ])
					: [null, null];
			mapData = geojson;
			mapDemandRows = parseDemandRowsPayload(instancePayload);
			if (!mapDemandRows.length) {
				mapDemandRows = parseDemandRows(demandCsv);
			}
		}

		if (syncStatus?.error) {
			state.update((draft) => ({ ...draft, errors: [syncStatus.error ?? '', ...draft.errors].slice(0, 12) }));
		}

		state.update((draft) => ({
			...draft,
			catalog,
			runsIndex,
			selectedInstanceId,
			selectedRunId,
			instanceManifest,
			instanceOverview,
			manifest: runCore[0] as FrontendManifest | null,
			overview: runCore[1] as OverviewPayload | null,
			pipelineProgress: runCore[2] as PipelineProgress | null,
			instancePayload,
			runConfig: runCore[3] as Record<string, unknown> | null,
			mapSummary,
			stage1: runCore[5] as Record<string, unknown> | null,
			stage2: runCore[6] as Record<string, unknown> | null,
			stage3: runCore[7] as Record<string, unknown> | null,
			activity: runCore[8] as Record<string, unknown> | null,
			alerts: (runCore[9] as { alerts?: AppState['alerts'] } | null)?.alerts ?? [],
			mapData,
			mapDemandRows,
			details: {
				stage1Clusters: detailUpdates.stage1Clusters ?? draft.details.stage1Clusters,
				stage2Scenarios: detailUpdates.stage2Scenarios ?? draft.details.stage2Scenarios,
				stage3Clusters: detailUpdates.stage3Clusters ?? draft.details.stage3Clusters,
				stage3Scopes: detailUpdates.stage3Scopes ?? draft.details.stage3Scopes
			},
			polling: {
				...draft.polling,
				inFlight: false,
				lastCompletedAt: new Date().toISOString(),
				lastSuccessfulAt: syncStatus?.lastSuccessfulAt ?? draft.polling.lastSuccessfulAt,
				fileCount: syncStatus?.fileCount ?? draft.polling.fileCount,
				consecutiveFailures: syncStatus?.error ? draft.polling.consecutiveFailures + 1 : 0
			}
		}));
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		state.update((draft) => ({
			...draft,
			errors: [message, ...draft.errors].slice(0, 12),
			polling: {
				...draft.polling,
				inFlight: false,
				lastCompletedAt: new Date().toISOString(),
				consecutiveFailures: draft.polling.consecutiveFailures + 1
			}
		}));
	}
}

export const appState = {
	subscribe: state.subscribe,
	start() {
		if (!syncWorker && typeof Worker !== 'undefined') {
			syncWorker = new Worker(new URL('$lib/file-sync-worker.ts', import.meta.url), { type: 'module' });
			syncWorker.onmessage = (event: MessageEvent<{ type: string }>) => {
				if (event.data.type === 'synced' || event.data.type === 'error') {
					void refreshFromMirror();
				}
			};
			syncWorker.postMessage({ type: 'start' });
		}
		if (!refreshHandle) {
			void refreshFromMirror();
			refreshHandle = setInterval(() => void refreshFromMirror(), POLL_INTERVAL_MS);
		}
	},
	stop() {
		if (refreshHandle) {
			clearInterval(refreshHandle);
			refreshHandle = null;
		}
		if (syncWorker) {
			syncWorker.postMessage({ type: 'stop' });
			syncWorker.terminate();
			syncWorker = null;
		}
	},
	selectInstance(instanceId: string | null) {
		state.update((draft) => ({ ...draft, selectedInstanceId: instanceId }));
		void refreshFromMirror();
	},
	selectRun(runId: string | null) {
		const catalog = get(state).catalog;
		const run = findRun(catalog, runId);
		state.update((draft) => ({
			...draft,
			selectedRunId: runId,
			selectedInstanceId: (run?.instance_id ?? draft.selectedInstanceId) || null
		}));
		void refreshFromMirror();
	},
	requestSync() {
		syncWorker?.postMessage({ type: 'sync-now' });
	},
	refreshNow() {
		void refreshFromMirror();
	},
	ensureMapData() {
		activeResources.add('map');
		void refreshFromMirror();
	},
	ensureStage1Cluster(clusterId: string) {
		activeDetails.add(`stage1:${clusterId}`);
		void refreshFromMirror();
	},
	ensureStage2Scenario(scenarioId: string) {
		activeDetails.add(`stage2:${scenarioId}`);
		void refreshFromMirror();
	},
	ensureStage3Cluster(clusterId: string) {
		activeDetails.add(`stage3cluster:${clusterId}`);
		void refreshFromMirror();
	},
	ensureStage3Scope(scopeId: string) {
		activeDetails.add(`stage3scope:${scopeId}`);
		void refreshFromMirror();
	}
};
