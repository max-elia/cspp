<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { apiFetch } from '$lib/api';
	import { appState } from '$lib/app-state';
	import {
		chooseClusterCount,
		runAngularGapPreview,
		runBrowserHierarchicalPreview,
		runBrowserKMeansPreview,
		runDepotRegularizedKMeansPreview,
		type AssignmentRow,
		type ClusterCentroid,
		type CustomerRecord
	} from '$lib/clustering-browser';
	import MetricGrid from '$lib/components/MetricGrid.svelte';
	import SimpleMap from '$lib/components/SimpleMap.svelte';
	import { formatNumber } from '$lib/format';
	import type {
		FeatureCollection,
		InstanceBundle,
		InstanceBundleCustomer,
		MapDemandRow,
		StageStatusMap
	} from '$lib/types';
	import { onMount } from 'svelte';

	type MapSummary = {
		warehouse?: { latitude?: number; longitude?: number } | null;
	};

	type WarehouseLocation = {
		latitude: number;
		longitude: number;
	};

	type AngularSliceBoundary = {
		angle: number;
		clusterBefore: number;
		clusterAfter: number;
	};

	type ClusterPreviewSummary = {
		clusterCount: number;
		minCustomers: number;
		maxCustomers: number;
		minDemandKg: number;
		maxDemandKg: number;
	};

	type PrepCustomerRow = {
		client_num: number;
		customer_id?: string | null;
		customer_name?: string | null;
		street?: string | null;
		postal_code?: string | null;
		city?: string | null;
		latitude: number;
		longitude: number;
		total_demand_kg: number;
		max_demand_kg: number;
		active_days: number;
		latest_demand_kg: number;
		cluster_id: number | null;
		super_cluster_id: number | null;
	};

	type CsppParameterConfig = {
		d_cost: number;
		h: number;
		max_tours_per_truck: number;
		charger_cost_multiplier: number;
	};

	type ClusteringMethod = 'kmeans' | 'depot_regularized_kmeans' | 'hierarchical_ward' | 'angular_gap';
	type SourceMode = 'upload' | 'create_existing' | null;
	type SetupStep = 'source' | 'customers' | 'clustering';
	type ServerRunEntry = {
		run_id?: string | null;
	};

	const TARGET_CLUSTER_SIZE = 16;
	const CLUSTER_ANIMATION_MS = 360;
	const workflowSteps: SetupStep[] = ['source', 'customers', 'clustering'];
	const DEFAULT_CSPP_PARAMS: CsppParameterConfig = {
		d_cost: 0.25,
		h: 50,
		max_tours_per_truck: 3,
		charger_cost_multiplier: 1
	};

	const runId = $derived(page.url.searchParams.get('source') ?? page.params.instanceId ?? page.params.runId ?? $appState.selectedInstanceId ?? null);

	let sourceMode = $state<SourceMode>('create_existing');
	let activeStep = $state<SetupStep>('source');

	let useRadius = $state(false);
	let selectionRadiusKm = $state(50);
	let selectionRadiusInitializedForRun = $state<string | null>(null);
	let manualExcludedClientNums = $state<Set<number>>(new Set());
	let manualIncludedClientNums = $state<Set<number>>(new Set());

	let newRunIdInput = $state('');
	let createStatus = $state('');
	let isCreating = $state(false);
	let bundleFile = $state<File | null>(null);
	let importStatus = $state('');
	let isImportingBundle = $state(false);
	let serverRunIds = $state<string[]>([]);

	let clusterSize = $state(TARGET_CLUSTER_SIZE);
	let clusterSettingsInitializedForRun = $state<string | null>(null);
	let clusteringMethod = $state<ClusteringMethod>('angular_gap');
	let demandAwareClustering = $state(true);
	let lastDemandAwareMethod = $state<ClusteringMethod>('angular_gap');
	let depotRegularizationLambda = $state(4);
	let clusteringStatus = $state('');
	let isRunningClustering = $state(false);
	let previewAssignments = $state<AssignmentRow[]>([]);
	let previewCentroids = $state<ClusterCentroid[]>([]);
	let previewSeed = $state<number | null>(null);
	let previewIteration = $state(0);
	let previewTotalSteps = $state(0);
	let previewConverged = $state(false);
	let previewMethod = $state<ClusteringMethod | null>(null);
	let previewHeight = $state<number | null>(null);
	let previewLargestGap = $state<number | null>(null);
	let previewConfigKey = $state('');
	let clusteringAnimationToken = 0;

	onMount(() => {
		appState.selectInstance(runId);
		appState.ensureMapData();
		void loadServerRuns();
	});

	const summary = $derived($appState.mapSummary as Record<string, unknown> | null);
	const overview = $derived((($appState.instanceOverview as Record<string, unknown> | null) ?? ($appState.overview as Record<string, unknown> | null)));
	const mapData = $derived($appState.mapData as FeatureCollection | null);
	const mapDemandRows = $derived($appState.mapDemandRows as MapDemandRow[]);
	const features = $derived(mapData?.features ?? []);
	const typedSummary = $derived((summary as MapSummary | null) ?? null);
	const sourceRunIsInstance = $derived.by(() => Boolean(overview?.instance_setup));

	const warehouse = $derived.by(() => {
		const latitude = Number(typedSummary?.warehouse?.latitude);
		const longitude = Number(typedSummary?.warehouse?.longitude);
		if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
		return { latitude, longitude };
	});

	function haversineKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
		const toRad = (value: number) => (value * Math.PI) / 180;
		const dLat = toRad(lat2 - lat1);
		const dLon = toRad(lon2 - lon1);
		const a =
			Math.sin(dLat / 2) ** 2 +
			Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
		return 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
	}

	function propsOf(feature: FeatureCollection['features'][number]): Record<string, unknown> {
		return feature.properties as Record<string, unknown>;
	}

	function asFiniteNumber(value: unknown, fallback = 0): number {
		const number = Number(value);
		return Number.isFinite(number) ? number : fallback;
	}

	function clusterValue(value: unknown): number | null {
		const number = Number(value);
		return Number.isFinite(number) ? number : null;
	}

	function sanitizeRunId(value: string): string {
		return value
			.trim()
			.replace(/[^A-Za-z0-9._-]+/g, '_')
			.replace(/_+/g, '_')
			.replace(/^_+|_+$/g, '');
	}

	function methodSlug(method: ClusteringMethod): string {
		if (method === 'depot_regularized_kmeans') return 'depot-kmeans';
		if (method === 'hierarchical_ward') return 'ward';
		if (method === 'angular_gap') return 'angular';
		return 'kmeans';
	}

	function methodSupportsDemandAware(method: ClusteringMethod): boolean {
		return method !== 'hierarchical_ward';
	}

	function pipelineClusteringMethod(method: ClusteringMethod): string {
		if (method === 'angular_gap') return 'angular_slices';
		if (method === 'kmeans' || method === 'depot_regularized_kmeans' || method === 'hierarchical_ward') return 'geographic';
		return method;
	}

	function defaultDemandAwareForMethod(method: ClusteringMethod): boolean {
		return method === 'angular_gap';
	}

	function buildDatedRunIdBase(
		date = new Date(),
		method: ClusteringMethod | null = null,
		targetClusterSize: number | null = null
	): string {
		const year = date.getFullYear();
		const month = String(date.getMonth() + 1).padStart(2, '0');
		const day = String(date.getDate()).padStart(2, '0');
		const parts = [`instance_${year}-${month}-${day}`];
		if (Number.isFinite(targetClusterSize) && targetClusterSize !== null && targetClusterSize > 0) {
			parts.push(`c${Math.round(targetClusterSize)}`);
		}
		if (method) {
			parts.push(methodSlug(method));
		}
		return sanitizeRunId(parts.join('_'));
	}

	function nextAvailableRunId(base: string): string {
		const existing = new Set(serverRunIds);
		let counter = 1;
		while (existing.has(`${base}_${counter}`)) {
			counter += 1;
		}
		return `${base}_${counter}`;
	}

	function resolveUniqueRunId(preferred: string): string {
		const sanitizedPreferred = sanitizeRunId(preferred);
		if (!sanitizedPreferred) return '';
		const existing = new Set([
			...serverRunIds,
			...(($appState.runsIndex?.runs ?? []).map((run) => run.run_id).filter(Boolean) as string[]),
			...(runId ? [runId] : [])
		]);
		if (!existing.has(sanitizedPreferred)) return sanitizedPreferred;
		let counter = 1;
		while (existing.has(`${sanitizedPreferred}_${counter}`)) {
			counter += 1;
		}
		return `${sanitizedPreferred}_${counter}`;
	}

	function csvEscape(value: unknown): string {
		const text = value === null || value === undefined ? '' : String(value);
		if (/[",\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
		return text;
	}

	function toCsv(headers: string[], rows: Array<Record<string, unknown>>): string {
		const lines = [headers.join(',')];
		for (const row of rows) {
			lines.push(headers.map((header) => csvEscape(row[header])).join(','));
		}
		return `${lines.join('\n')}\n`;
	}

	function jsonText(payload: unknown): string {
		return JSON.stringify(payload, null, 2);
	}

	function buildRunsIndexEntry(options: {
		targetRunId: string;
		nowIso: string;
		clusteringMethodValue: string | null;
		maxDistanceFromWarehouseKm: number | null;
		stageStatusPayload: StageStatusMap;
	}): Record<string, unknown> {
		const { targetRunId, nowIso, clusteringMethodValue, maxDistanceFromWarehouseKm, stageStatusPayload } =
			options;
		return {
			run_id: targetRunId,
			label: targetRunId,
			latest: true,
			run_last_modified_at: nowIso,
			last_stage_recorded: null,
			clustering_method: clusteringMethodValue,
			max_distance_from_warehouse_km: maxDistanceFromWarehouseKm,
			stage_status: stageStatusPayload,
			frontend_manifest_available: true
		};
	}

	function buildRunsIndexPayload(options: {
		targetRunId: string;
		nowIso: string;
		clusteringMethodValue: string | null;
		maxDistanceFromWarehouseKm: number | null;
		stageStatusPayload: StageStatusMap;
	}): Record<string, unknown> {
		const entry = buildRunsIndexEntry(options);
		const existingRuns = ($appState.runsIndex?.runs ?? []).filter((run) => run.run_id !== options.targetRunId);
		return {
			schema_version: $appState.runsIndex?.schema_version ?? 1,
			updated_at: options.nowIso,
			latest_run_id: options.targetRunId,
			runs: [
				entry,
				...existingRuns.map((run) => ({
					...run,
					latest: false
				}))
			]
		};
	}

	function buildAssignmentLookup(rows: AssignmentRow[] | null | undefined): Map<number, { cluster_id: number; super_cluster_id: number }> {
		const lookup = new Map<number, { cluster_id: number; super_cluster_id: number }>();
		for (const row of rows ?? []) {
			lookup.set(row.client_num, {
				cluster_id: row.cluster_id,
				super_cluster_id: row.super_cluster_id
			});
		}
		return lookup;
	}

	function applyAssignmentsToFeatures(
		sourceFeatures: FeatureCollection['features'],
		rows: AssignmentRow[] | null | undefined,
		clearMissing = false
	): FeatureCollection['features'] {
		const lookup = buildAssignmentLookup(rows);
		return sourceFeatures.map((feature) => {
			const properties = propsOf(feature);
			const next = lookup.get(asFiniteNumber(properties.client_num));
			return {
				...feature,
				properties: {
					...properties,
					cluster_id: next?.cluster_id ?? (clearMissing ? null : clusterValue(properties.cluster_id)),
					super_cluster_id: next?.super_cluster_id ?? (clearMissing ? null : clusterValue(properties.super_cluster_id))
				} as Record<string, unknown>
			};
		});
	}

	function clearAssignmentsFromFeatures(
		sourceFeatures: FeatureCollection['features']
	): FeatureCollection['features'] {
		return sourceFeatures.map((feature) => ({
			...feature,
			properties: {
				...propsOf(feature),
				cluster_id: null,
				super_cluster_id: null
			}
		}));
	}

	function buildPrepCustomers(sourceFeatures: FeatureCollection['features']): PrepCustomerRow[] {
		return sourceFeatures.map((feature) => {
			const properties = propsOf(feature);
			return {
				client_num: asFiniteNumber(properties.client_num),
				customer_id: (properties.customer_id as string | null | undefined) ?? null,
				customer_name: (properties.customer_name as string | null | undefined) ?? null,
				street: (properties.address as string | null | undefined) ?? null,
				postal_code: (properties.postal_code as string | null | undefined) ?? null,
				city: (properties.city as string | null | undefined) ?? null,
				latitude: feature.geometry.coordinates[1],
				longitude: feature.geometry.coordinates[0],
				total_demand_kg: asFiniteNumber(properties.total_demand),
				max_demand_kg: asFiniteNumber(properties.max_demand),
				active_days: asFiniteNumber(properties.nonzero_days),
				latest_demand_kg: asFiniteNumber(properties.latest_demand),
				cluster_id: clusterValue(properties.cluster_id),
				super_cluster_id: clusterValue(properties.super_cluster_id)
			};
		});
	}

	function buildCustomerRecords(sourceFeatures: FeatureCollection['features']): CustomerRecord[] {
		return sourceFeatures.map((feature) => {
			const properties = propsOf(feature);
			return {
				client_num: asFiniteNumber(properties.client_num),
				customer_id: (properties.customer_id as string | null | undefined) ?? null,
				customer_name: (properties.customer_name as string | null | undefined) ?? null,
				latitude: feature.geometry.coordinates[1],
				longitude: feature.geometry.coordinates[0],
				cluster_id: clusterValue(properties.cluster_id),
				super_cluster_id: clusterValue(properties.super_cluster_id),
				total_demand_kg: asFiniteNumber(properties.total_demand)
			};
		});
	}

	function buildCentroidsFromAssignments(customers: CustomerRecord[], rows: AssignmentRow[]): ClusterCentroid[] {
		const grouped = new Map<number, { latitude: number; longitude: number; size: number }>();
		const customersByClient = new Map(customers.map((customer) => [customer.client_num, customer]));
		for (const row of rows) {
			const customer = customersByClient.get(row.client_num);
			if (!customer) continue;
			const current = grouped.get(row.cluster_id) ?? { latitude: 0, longitude: 0, size: 0 };
			current.latitude += customer.latitude;
			current.longitude += customer.longitude;
			current.size += 1;
			grouped.set(row.cluster_id, current);
		}
		return Array.from(grouped.entries())
			.sort((a, b) => a[0] - b[0])
			.map(([cluster_id, value]) => ({
				cluster_id,
				latitude: value.latitude / Math.max(1, value.size),
				longitude: value.longitude / Math.max(1, value.size),
				size: value.size
			}));
	}

	function buildAngularSliceBoundaries(
		customers: CustomerRecord[],
		rows: AssignmentRow[],
		center: WarehouseLocation | null
	): AngularSliceBoundary[] {
		if (!center || customers.length < 2 || rows.length < 2) return [];
		const clusterByClient = new Map(rows.map((row) => [row.client_num, row.cluster_id]));
		const sorted = customers
			.map((customer) => ({
				clientNum: customer.client_num,
				clusterId: clusterByClient.get(customer.client_num),
				angle: Math.atan2(customer.latitude - center.latitude, customer.longitude - center.longitude)
			}))
			.filter((entry): entry is { clientNum: number; clusterId: number; angle: number } => Number.isFinite(entry.clusterId))
			.sort((a, b) => a.angle - b.angle);
		if (sorted.length < 2) return [];

		const boundaries: AngularSliceBoundary[] = [];
		for (let index = 0; index < sorted.length; index += 1) {
			const current = sorted[index];
			const next = sorted[(index + 1) % sorted.length];
			if (current.clusterId === next.clusterId) continue;
			let gap = next.angle - current.angle;
			if (gap < 0) gap += 2 * Math.PI;
			let boundaryAngle = current.angle + gap / 2;
			if (boundaryAngle > Math.PI) boundaryAngle -= 2 * Math.PI;
			boundaries.push({
				angle: boundaryAngle,
				clusterBefore: current.clusterId,
				clusterAfter: next.clusterId
			});
		}
		return boundaries;
	}

	function buildClusterCounts(sourceFeatures: FeatureCollection['features']): Record<string, number> {
		const counts = new Map<number, number>();
		for (const feature of sourceFeatures) {
			const clusterId = clusterValue(propsOf(feature).cluster_id);
			if (clusterId === null || clusterId < 0) continue;
			counts.set(clusterId, (counts.get(clusterId) ?? 0) + 1);
		}
		return Object.fromEntries(Array.from(counts.entries()).sort((a, b) => a[0] - b[0]).map(([clusterId, count]) => [String(clusterId), count]));
	}

	function buildMapSummaryPayload(
		nowIso: string,
		sourceFeatures: FeatureCollection['features'],
		demandRows: MapDemandRow[],
		warehouseLocation: WarehouseLocation | null = warehouse
	): Record<string, unknown> {
		const warehousePayload = warehouseLocation ? { latitude: warehouseLocation.latitude, longitude: warehouseLocation.longitude } : null;
		const longitudes = sourceFeatures.map((feature) => feature.geometry.coordinates[0]);
		const latitudes = sourceFeatures.map((feature) => feature.geometry.coordinates[1]);
		if (warehouseLocation) {
			longitudes.push(warehouseLocation.longitude);
			latitudes.push(warehouseLocation.latitude);
		}
		const demandValues = sourceFeatures.map((feature) => asFiniteNumber(propsOf(feature).total_demand));
		return {
			updated_at: nowIso,
			customer_count: sourceFeatures.length,
			warehouse: warehousePayload,
			bounds:
				latitudes.length && longitudes.length
					? {
							min_latitude: Math.min(...latitudes),
							max_latitude: Math.max(...latitudes),
							min_longitude: Math.min(...longitudes),
							max_longitude: Math.max(...longitudes)
						}
					: null,
			available_demand_dates: Array.from(new Set(demandRows.map((row) => row.delivery_date))).sort(),
			cluster_counts: buildClusterCounts(sourceFeatures),
			demand_min: demandValues.length ? Math.min(...demandValues) : null,
			demand_max: demandValues.length ? Math.max(...demandValues) : null
		};
	}

	function buildDefaultStageStatus(): StageStatusMap {
		return {
			first_stage: 'missing',
			scenario_evaluation: 'missing',
			reoptimization: 'missing'
		};
	}

	function buildDemandRowsPayload(rows: MapDemandRow[]): Array<Record<string, unknown>> {
		return rows.map((row) => ({
			delivery_date: row.delivery_date,
			client_num: row.client_num,
			customer_id: row.customer_id ?? null,
			demand_kg: row.demand_kg
		}));
	}

	function buildClusterPreviewSummary(
		customers: CustomerRecord[],
		rows: AssignmentRow[]
	): ClusterPreviewSummary | null {
		if (!customers.length || !rows.length) return null;
		const demandByClient = new Map(
			customers.map((customer) => [customer.client_num, asFiniteNumber(customer.total_demand_kg)])
		);
		const grouped = new Map<number, { customers: number; demandKg: number }>();
		for (const row of rows) {
			const current = grouped.get(row.cluster_id) ?? { customers: 0, demandKg: 0 };
			current.customers += 1;
			current.demandKg += demandByClient.get(row.client_num) ?? 0;
			grouped.set(row.cluster_id, current);
		}
		if (!grouped.size) return null;
		const clusters = Array.from(grouped.values());
		return {
			clusterCount: grouped.size,
			minCustomers: Math.min(...clusters.map((cluster) => cluster.customers)),
			maxCustomers: Math.max(...clusters.map((cluster) => cluster.customers)),
			minDemandKg: Math.min(...clusters.map((cluster) => cluster.demandKg)),
			maxDemandKg: Math.max(...clusters.map((cluster) => cluster.demandKg))
		};
	}

	function buildFeaturesFromPrepCustomers(customers: PrepCustomerRow[]): FeatureCollection['features'] {
		return customers.map((customer) => ({
			type: 'Feature',
			geometry: {
				type: 'Point',
				coordinates: [customer.longitude, customer.latitude]
			},
			properties: {
				client_num: customer.client_num,
				customer_id: customer.customer_id ?? null,
				customer_name: customer.customer_name ?? null,
				address: customer.street ?? null,
				postal_code: customer.postal_code ?? null,
				city: customer.city ?? null,
				total_demand: customer.total_demand_kg,
				max_demand: customer.max_demand_kg,
				nonzero_days: customer.active_days,
				latest_demand: customer.latest_demand_kg,
				cluster_id: customer.cluster_id ?? null,
				super_cluster_id: customer.super_cluster_id ?? null
			}
		}));
	}

	function buildAssignmentsFromPrepCustomers(customers: PrepCustomerRow[]): AssignmentRow[] {
		const clusteredCustomers = customers.filter(
			(customer) => customer.cluster_id !== null && customer.cluster_id !== undefined
		);
		if (!clusteredCustomers.length) return [];
		if (clusteredCustomers.length !== customers.length) {
			throw new Error('Imported bundle must provide either no cluster assignments or assignments for every store.');
		}
		return clusteredCustomers.map((customer) => ({
			client_num: customer.client_num,
			customer_id: customer.customer_id ?? undefined,
			cluster_id: customer.cluster_id as number,
			super_cluster_id: customer.super_cluster_id ?? 0
		}));
	}

	function parseInstanceBundle(text: string): {
		bundle: InstanceBundle;
		prepCustomers: PrepCustomerRow[];
		demandRows: MapDemandRow[];
		warehouseLocation: WarehouseLocation | null;
	} {
		const payload = JSON.parse(text) as InstanceBundle;
		if (!Array.isArray(payload.customers) || payload.customers.length < 1) {
			throw new Error('Bundle must include a non-empty stores/customers array.');
		}
		if (!Array.isArray(payload.demand_rows)) {
			throw new Error('Bundle must include a demand_rows array.');
		}

		const warehouseLatitude = Number(payload.warehouse?.latitude);
		const warehouseLongitude = Number(payload.warehouse?.longitude);
		const warehouseLocation =
			Number.isFinite(warehouseLatitude) && Number.isFinite(warehouseLongitude)
				? { latitude: warehouseLatitude, longitude: warehouseLongitude }
				: null;

		const prepCustomers = payload.customers.map((customer: InstanceBundleCustomer) => {
			const latitude = Number(customer.latitude);
			const longitude = Number(customer.longitude);
			const clientNum = Number(customer.client_num);
			if (!Number.isFinite(clientNum) || !Number.isFinite(latitude) || !Number.isFinite(longitude)) {
				throw new Error('Bundle stores/customers must include valid client_num, latitude, and longitude values.');
			}
			return {
				client_num: clientNum,
				customer_id: customer.customer_id ?? null,
				customer_name: customer.customer_name ?? null,
				street: customer.street ?? null,
				postal_code: customer.postal_code ?? null,
				city: customer.city ?? null,
				latitude,
				longitude,
				total_demand_kg: asFiniteNumber(customer.total_demand_kg),
				max_demand_kg: asFiniteNumber(customer.max_demand_kg),
				active_days: asFiniteNumber(customer.active_days),
				latest_demand_kg: asFiniteNumber(customer.latest_demand_kg),
				cluster_id: clusterValue(customer.cluster_id),
				super_cluster_id: clusterValue(customer.super_cluster_id)
			};
		});

		const demandRows = payload.demand_rows.map((row) => {
			const clientNum = Number(row.client_num);
			const demandKg = Number(row.demand_kg);
			if (!Number.isFinite(clientNum) || !Number.isFinite(demandKg)) {
				throw new Error('Bundle demand_rows must include valid client_num and demand_kg values.');
			}
			return {
				delivery_date: String(row.delivery_date ?? ''),
				client_num: clientNum,
				customer_id: row.customer_id ?? null,
				demand_kg: demandKg
			};
		});

		return { bundle: payload, prepCustomers, demandRows, warehouseLocation };
	}

	function sleep(ms: number): Promise<void> {
		return new Promise((resolve) => {
			window.setTimeout(resolve, ms);
		});
	}

	async function persistInstanceRun({
		targetRunId,
		sourceRunId,
		prepCustomers,
		demandRows,
		mapFeatures,
		assignmentRows,
		warehouseLocation,
		clusteringMethodValue,
		maxDistanceFromWarehouseKm,
		instanceSetupPayload,
		navigateTo
	}: {
		targetRunId: string;
		sourceRunId: string | null;
		prepCustomers: PrepCustomerRow[];
		demandRows: MapDemandRow[];
		mapFeatures: FeatureCollection['features'];
		assignmentRows: AssignmentRow[];
		warehouseLocation: WarehouseLocation | null;
		clusteringMethodValue: string | null;
		maxDistanceFromWarehouseKm: number | null;
		instanceSetupPayload: Record<string, unknown>;
		navigateTo?: string;
	}): Promise<void> {
		const response = await apiFetch('/api/instances', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				instance_id: targetRunId,
				source_instance_id: sourceRunId,
				clustering_method: clusteringMethodValue,
				max_distance_from_warehouse_km: maxDistanceFromWarehouseKm,
				warehouse: warehouseLocation
					? {
							latitude: warehouseLocation.latitude,
							longitude: warehouseLocation.longitude
						}
					: null,
				customers: prepCustomers,
				demand_rows: buildDemandRowsPayload(demandRows),
				assignments: assignmentRows,
				instance_setup: instanceSetupPayload,
				map_features: mapFeatures
			})
		});
		const text = await response.text();
		const payload = text ? (JSON.parse(text) as Record<string, unknown>) : {};
		if (!response.ok) {
			throw new Error(typeof payload.detail === 'string' ? payload.detail : `Create instance failed: ${response.status}`);
		}
		serverRunIds = Array.from(new Set([...serverRunIds, targetRunId])).sort((a, b) => a.localeCompare(b));
		appState.selectInstance(targetRunId);
		appState.requestSync();
		await goto(navigateTo ?? `/instances/${targetRunId}/runs`);
	}

	const customersWithDistance = $derived.by(() =>
		features.map((feature) => {
			const [longitude, latitude] = feature.geometry.coordinates;
			return {
				feature,
				clientNum: asFiniteNumber(propsOf(feature).client_num),
				totalDemand: asFiniteNumber(propsOf(feature).total_demand),
				distanceKm: warehouse ? haversineKm(warehouse.latitude, warehouse.longitude, latitude, longitude) : null
			};
		})
	);

	const maxRadiusKm = $derived.by(() => {
		let max = 0;
		for (const customer of customersWithDistance) {
			max = Math.max(max, customer.distanceKm ?? 0);
		}
		return Math.max(1, Math.ceil(max));
	});

	const activeRadiusKm = $derived.by(() => (useRadius ? selectionRadiusKm : null));

	function isWithinActiveRadius(distanceKm: number | null): boolean {
		if (!useRadius) return true;
		if (distanceKm === null) return false;
		return distanceKm <= selectionRadiusKm + 1e-9;
	}

	const includedCustomers = $derived.by(() =>
		customersWithDistance.filter((customer) => {
			if (manualExcludedClientNums.has(customer.clientNum)) return false;
			if (isWithinActiveRadius(customer.distanceKm)) return true;
			return manualIncludedClientNums.has(customer.clientNum);
		})
	);

	const includedClientNums = $derived.by(() => new Set(includedCustomers.map((customer) => customer.clientNum)));
	const includedBaseFeatures = $derived.by(() =>
		features.filter((feature) => includedClientNums.has(asFiniteNumber(propsOf(feature).client_num)))
	);
	const clusteringSourceFeatures = $derived.by(() =>
		sourceMode === 'create_existing' && sourceRunIsInstance
			? clearAssignmentsFromFeatures(includedBaseFeatures)
			: includedBaseFeatures
	);
	const selectedMapData = $derived.by(
		(): FeatureCollection => ({
			type: 'FeatureCollection',
			features: clusteringSourceFeatures
		})
	);
	const filteredDemandRows = $derived.by(() =>
		mapDemandRows.filter((row) => includedClientNums.has(row.client_num))
	);
	const includedCustomerCount = $derived(includedCustomers.length);
	const excludedCustomerCount = $derived(features.length - includedCustomerCount);
	const includedDemandKg = $derived.by(() =>
		includedCustomers.reduce((total, customer) => total + customer.totalDemand, 0)
	);
	const totalDemandKg = $derived.by(() =>
		customersWithDistance.reduce((total, customer) => total + customer.totalDemand, 0)
	);
	const includedShare = $derived.by(() => (totalDemandKg > 0 ? (includedDemandKg / totalDemandKg) * 100 : 0));
	const selectedCustomers = $derived.by(() => buildCustomerRecords(clusteringSourceFeatures));
	const suggestedClusterCount = $derived.by(() =>
		chooseClusterCount(selectedCustomers.length, Math.max(1, clusterSize || TARGET_CLUSTER_SIZE))
	);
	const filteredMetrics = $derived([
		{ label: 'Selected', value: includedCustomerCount },
		{ label: 'Excluded', value: excludedCustomerCount },
		{ label: 'Demand kg', value: includedDemandKg },
		{ label: 'Demand Share %', value: Number(includedShare.toFixed(1)) }
	]);
	const availableInstances = $derived.by(() => {
		const runIds = new Set<string>(serverRunIds);
		for (const instance of $appState.catalog?.instances ?? []) {
			if (instance.instance_id) runIds.add(instance.instance_id);
		}
		if (runId) runIds.add(runId);
		return Array.from(runIds).sort((a, b) => a.localeCompare(b));
	});

	const suggestedRunId = $derived.by(() => {
		return nextAvailableRunId(buildDatedRunIdBase(new Date(), clusteringMethod, clusterSize));
	});

	const currentPreviewKey = $derived.by(() =>
		JSON.stringify({
			sourceRunId: runId,
			sourceMode,
			useRadius,
			selectionRadiusKm: useRadius ? selectionRadiusKm : null,
			excluded: Array.from(manualExcludedClientNums).sort((a, b) => a - b),
			clusteringMethod,
			clusterSize,
			demandAwareClustering,
			depotRegularizationLambda,
			selectedClients: Array.from(includedClientNums).sort((a, b) => a - b)
		})
	);
	const previewIsFresh = $derived.by(() => previewAssignments.length > 0 && previewConfigKey === currentPreviewKey);
	const displayedAssignments = $derived.by(() => previewAssignments);
	const displayedCentroids = $derived.by(() => previewCentroids);
	const displayedAngularSliceBoundaries = $derived.by(() =>
		previewMethod === 'angular_gap' && warehouse
			? buildAngularSliceBoundaries(selectedCustomers, displayedAssignments, warehouse)
			: []
	);
	const selectedBaseClusterMap = $derived.by(
		() =>
			new Map(
				clusteringSourceFeatures
					.map((feature) => {
						const clusterId = clusterValue(propsOf(feature).cluster_id);
						if (clusterId === null) return null;
						return [asFiniteNumber(propsOf(feature).client_num), clusterId] as const;
					})
					.filter((entry): entry is readonly [number, number] => entry !== null)
			)
	);
	const displayedClusterMap = $derived.by(() => new Map(displayedAssignments.map((row) => [row.client_num, row.cluster_id])));
	const clusteringStepClusterMap = $derived.by(() =>
		displayedAssignments.length ? displayedClusterMap : selectedBaseClusterMap
	);
	const previewClusterCount = $derived.by(() => new Set(displayedAssignments.map((row) => row.cluster_id)).size);
	const previewClusterSummary = $derived.by(() =>
		buildClusterPreviewSummary(selectedCustomers, displayedAssignments)
	);

	$effect(() => {
		if (!runId) {
			sourceMode = 'upload';
		}
		if (runId && selectionRadiusInitializedForRun !== runId && maxRadiusKm > 0) {
			selectionRadiusInitializedForRun = runId;
			selectionRadiusKm = maxRadiusKm;
			manualExcludedClientNums = new Set();
			manualIncludedClientNums = new Set();
			newRunIdInput = '';
			createStatus = '';
			clusteringStatus = '';
			sourceMode = 'create_existing';
			const requestedStep = page.url.searchParams.get('step');
			if (!requestedStep) {
				activeStep = 'source';
			}
		}
	});

	$effect(() => {
		const requestedStep = page.url.searchParams.get('step');
		if (!runId || (sourceMode !== 'create_existing' && sourceMode !== 'upload')) return;
		if (requestedStep === 'customers' && features.length) {
			activeStep = 'customers';
		}
		if (requestedStep === 'clustering' && includedCustomerCount > 0) {
			activeStep = 'clustering';
		}
	});

	$effect(() => {
		if (selectionRadiusKm > maxRadiusKm) {
			selectionRadiusKm = maxRadiusKm;
		}
	});

	$effect(() => {
		if (!runId) return;
		if (clusterSettingsInitializedForRun !== runId) {
			clusterSettingsInitializedForRun = runId;
			clusterSize = TARGET_CLUSTER_SIZE;
			demandAwareClustering = defaultDemandAwareForMethod(clusteringMethod);
			lastDemandAwareMethod = clusteringMethod;
			depotRegularizationLambda = Math.max(1, Math.round(TARGET_CLUSTER_SIZE * 0.25));
			previewAssignments = [];
			previewCentroids = [];
			previewSeed = null;
			previewIteration = 0;
			previewTotalSteps = 0;
			previewConverged = false;
			previewMethod = null;
			previewHeight = null;
			previewLargestGap = null;
			previewConfigKey = '';
			return;
		}
		if (clusterSize < 1) {
			clusterSize = 1;
		}
	});

	$effect(() => {
		if (lastDemandAwareMethod === clusteringMethod) return;
		lastDemandAwareMethod = clusteringMethod;
		demandAwareClustering = methodSupportsDemandAware(clusteringMethod)
			? defaultDemandAwareForMethod(clusteringMethod)
			: false;
	});

	function resetPreview(message = ''): void {
		clusteringAnimationToken += 1;
		previewAssignments = [];
		previewCentroids = [];
		previewSeed = null;
		previewIteration = 0;
		previewTotalSteps = 0;
		previewConverged = false;
		previewMethod = null;
		previewHeight = null;
		previewLargestGap = null;
		previewConfigKey = '';
		isRunningClustering = false;
		if (message) clusteringStatus = message;
	}

	function toggleCustomer(clientNum: number): void {
		const customer = customersWithDistance.find((entry) => entry.clientNum === clientNum);
		if (!customer) return;

		if (isWithinActiveRadius(customer.distanceKm)) {
			const nextExcluded = new Set(manualExcludedClientNums);
			if (nextExcluded.has(clientNum)) {
				nextExcluded.delete(clientNum);
			} else {
				nextExcluded.add(clientNum);
			}
			manualExcludedClientNums = nextExcluded;
			if (manualIncludedClientNums.has(clientNum)) {
				const nextIncluded = new Set(manualIncludedClientNums);
				nextIncluded.delete(clientNum);
				manualIncludedClientNums = nextIncluded;
			}
		} else {
			const nextIncluded = new Set(manualIncludedClientNums);
			if (nextIncluded.has(clientNum)) {
				nextIncluded.delete(clientNum);
			} else {
				nextIncluded.add(clientNum);
			}
			manualIncludedClientNums = nextIncluded;
			if (manualExcludedClientNums.has(clientNum)) {
				const nextExcluded = new Set(manualExcludedClientNums);
				nextExcluded.delete(clientNum);
				manualExcludedClientNums = nextExcluded;
			}
		}
		resetPreview();
	}

	function setRadiusEnabled(value: boolean): void {
		useRadius = value;
		resetPreview(value ? 'Radius filter enabled. Run clustering again.' : 'Radius filter disabled. Run clustering again.');
	}

	function updateRadius(value: number): void {
		selectionRadiusKm = value;
		resetPreview();
	}

	function resetSelection(): void {
		manualExcludedClientNums = new Set();
		manualIncludedClientNums = new Set();
		useRadius = false;
		selectionRadiusKm = maxRadiusKm;
		resetPreview('Selection reset.');
	}

	function goToStep(step: SetupStep): void {
		if (!workflowSteps.includes(step)) return;
		const canFilter = sourceMode === 'create_existing' || sourceMode === 'upload';
		if (step === 'customers' && !canFilter) return;
		if (step === 'clustering' && (!canFilter || includedCustomerCount < 1)) return;
		activeStep = step;
		const url = new URL(page.url);
		if (step === 'source') {
			url.searchParams.delete('step');
		} else {
			url.searchParams.set('step', step);
		}
		goto(url.toString(), { replaceState: true, noScroll: true });
	}

	async function loadServerRuns(): Promise<void> {
		try {
			const response = await apiFetch('/api/instances');
			if (!response.ok) return;
			const payload = (await response.json()) as { instances?: Array<{ instance_id?: string | null }> };
			serverRunIds = (payload.instances ?? [])
				.map((run) => run.instance_id ?? '')
				.filter((value): value is string => Boolean(value));
		} catch {
			serverRunIds = serverRunIds;
		}
	}

	async function openSourceRun(targetRunId: string): Promise<void> {
		sourceMode = 'create_existing';
		if (!targetRunId) return;
		if (targetRunId === runId) {
			goToStep('customers');
			return;
		}
		await goto(`/instances/new?source=${encodeURIComponent(targetRunId)}&step=customers`);
	}

	async function handleBundleFileChange(event: Event): Promise<void> {
		const file = (event.currentTarget as HTMLInputElement).files?.[0] ?? null;
		bundleFile = file;
		importStatus = '';
		if (!file) return;
		sourceMode = 'upload';
		await importInstanceBundle(file);
	}

	async function runClusteringPreview(): Promise<void> {
		if (!selectedCustomers.length) {
			clusteringStatus = 'Select at least one store.';
			return;
		}

		const targetClusterSize = Math.max(1, clusterSize || TARGET_CLUSTER_SIZE);
		clusterSize = targetClusterSize;
		const requestedClusterCount = Math.max(
			1,
			Math.min(chooseClusterCount(selectedCustomers.length, targetClusterSize) || 1, selectedCustomers.length)
		);
		const token = ++clusteringAnimationToken;
		isRunningClustering = true;
		previewAssignments = [];
		previewCentroids = [];
		previewSeed = null;
		previewIteration = 0;
		previewTotalSteps = 0;
		previewConverged = false;
		previewMethod = clusteringMethod;
		previewHeight = null;
		previewLargestGap = null;
		previewConfigKey = '';

		try {
			if (clusteringMethod === 'kmeans') {
				const result = runBrowserKMeansPreview(selectedCustomers, requestedClusterCount, {
					maxIterations: 30,
					demandAware: demandAwareClustering
				});
				previewSeed = result.seed;
				previewTotalSteps = result.steps.length;

				if (!result.steps.length) {
					previewAssignments = result.finalAssignments;
					previewCentroids = result.finalCentroids;
					previewIteration = result.iterations;
					previewConverged = result.converged;
				} else {
					for (const [index, step] of result.steps.entries()) {
						if (token !== clusteringAnimationToken) return;
						previewAssignments = step.assignments;
						previewCentroids = step.centroids;
						previewIteration = step.iteration;
						previewConverged = step.converged;
						clusteringStatus = `Animating ${demandAwareClustering ? 'demand-aware ' : ''}k-means step ${index + 1}/${result.steps.length} with seed ${result.seed}.`;
						if (index < result.steps.length - 1) {
							await sleep(CLUSTER_ANIMATION_MS);
						}
					}
				}

				clusteringStatus = `${demandAwareClustering ? 'Demand-aware ' : ''}k-means preview ready: ${requestedClusterCount} clusters, ${previewIteration} iterations, seed ${result.seed}.`;
			} else if (clusteringMethod === 'depot_regularized_kmeans') {
				if (!warehouse) {
					clusteringStatus = 'Warehouse coordinates are missing.';
					return;
				}
				const lambda = Math.max(0, depotRegularizationLambda);
				const result = runDepotRegularizedKMeansPreview(selectedCustomers, requestedClusterCount, warehouse, {
					maxIterations: 30,
					lambda,
					demandAware: demandAwareClustering
				});
				previewSeed = result.seed;
				previewTotalSteps = result.steps.length;

				if (!result.steps.length) {
					previewAssignments = result.finalAssignments;
					previewCentroids = result.finalCentroids;
					previewIteration = result.iterations;
					previewConverged = result.converged;
				} else {
					for (const [index, step] of result.steps.entries()) {
						if (token !== clusteringAnimationToken) return;
						previewAssignments = step.assignments;
						previewCentroids = step.centroids;
						previewIteration = step.iteration;
						previewConverged = step.converged;
						clusteringStatus = `Animating ${demandAwareClustering ? 'demand-aware ' : ''}depot-regularized k-means step ${index + 1}/${result.steps.length} with seed ${result.seed} and lambda ${formatNumber(lambda, 2)}.`;
						if (index < result.steps.length - 1) {
							await sleep(CLUSTER_ANIMATION_MS);
						}
					}
				}

				clusteringStatus = `${demandAwareClustering ? 'Demand-aware ' : ''}depot-regularized k-means preview ready: ${requestedClusterCount} clusters, ${previewIteration} iterations, seed ${result.seed}, lambda ${formatNumber(lambda, 2)}.`;
			} else if (clusteringMethod === 'hierarchical_ward') {
				const result = runBrowserHierarchicalPreview(selectedCustomers, requestedClusterCount, {
					method: 'ward',
					maxAnimationSteps: 18
				});
				previewTotalSteps = result.steps.length;
				previewHeight = result.finalHeight;

				if (!result.steps.length) {
					previewAssignments = result.finalAssignments;
					previewCentroids = result.finalCentroids;
					previewIteration = result.iterations;
					previewConverged = result.converged;
				} else {
					for (const [index, step] of result.steps.entries()) {
						if (token !== clusteringAnimationToken) return;
						previewAssignments = step.assignments;
						previewCentroids = step.centroids;
						previewIteration = step.iteration;
						previewConverged = step.converged;
						clusteringStatus = `Animating hierarchical Ward clustering step ${index + 1}/${result.steps.length}.`;
						if (index < result.steps.length - 1) {
							await sleep(CLUSTER_ANIMATION_MS);
						}
					}
				}

				clusteringStatus = `Hierarchical Ward preview ready: ${requestedClusterCount} clusters, ${previewTotalSteps} animated cuts, root height ${formatNumber(result.finalHeight, 3)}.`;
			} else if (clusteringMethod === 'angular_gap') {
				if (!warehouse) {
					clusteringStatus = 'Warehouse coordinates are missing.';
					return;
				}
				const result = runAngularGapPreview(selectedCustomers, requestedClusterCount, warehouse, {
					demandAware: demandAwareClustering
				});
				previewTotalSteps = result.steps.length;
				previewLargestGap = result.selectedGapSizes[0] ?? null;

				if (!result.steps.length) {
					previewAssignments = result.finalAssignments;
					previewCentroids = result.finalCentroids;
					previewIteration = result.iterations;
					previewConverged = result.converged;
				} else {
					for (const [index, step] of result.steps.entries()) {
						if (token !== clusteringAnimationToken) return;
						previewAssignments = step.assignments;
						previewCentroids = step.centroids;
						previewIteration = step.iteration;
						previewConverged = step.converged;
						clusteringStatus = `Animating ${demandAwareClustering ? 'demand-aware ' : ''}angular slices step ${index + 1}/${result.steps.length}.`;
						if (index < result.steps.length - 1) {
							await sleep(CLUSTER_ANIMATION_MS);
						}
					}
				}

				clusteringStatus = `${demandAwareClustering ? 'Demand-aware ' : ''}angular slices preview ready: ${requestedClusterCount} slices, largest gap ${previewLargestGap !== null ? formatNumber((previewLargestGap * 180) / Math.PI, 1) : 'n/a'} deg.`;
			}

			previewConfigKey = currentPreviewKey;
		} catch (error) {
			clusteringStatus = error instanceof Error ? error.message : String(error);
		} finally {
			if (token === clusteringAnimationToken) {
				isRunningClustering = false;
			}
		}
	}

	async function createClusteredInstance(): Promise<void> {
		const targetRunId = sanitizeRunId(newRunIdInput) || suggestedRunId;
		if (!runId) {
			createStatus = 'No source instance selected.';
			return;
		}
		if (sourceMode !== 'create_existing' && sourceMode !== 'upload') {
			createStatus = 'Choose "Create from existing" or upload a bundle to generate a new instance.';
			return;
		}
		if (!targetRunId) {
			createStatus = 'Enter an instance id.';
			return;
		}
		if (!includedBaseFeatures.length) {
			createStatus = 'Select at least one store.';
			return;
		}
		if (!displayedAssignments.length) {
			createStatus = 'Run clustering for the current selection before creating the instance.';
			return;
		}
		if (serverRunIds.includes(targetRunId)) {
			createStatus = `Instance ${targetRunId} already exists.`;
			return;
		}

		isCreating = true;
		createStatus = 'Creating instance...';

		try {
			const mapFeatures = applyAssignmentsToFeatures(clusteringSourceFeatures, displayedAssignments, true);
			const prepCustomers = buildPrepCustomers(mapFeatures);
			const clusterCountValue = new Set(displayedAssignments.map((row) => row.cluster_id)).size;
			const clusteringMethodValue = pipelineClusteringMethod(clusteringMethod);
			const instanceSetupPayload = {
					source_run_id: runId,
					max_distance_from_warehouse_km: activeRadiusKm,
					radius_enabled: useRadius,
					included_customer_count: prepCustomers.length,
					excluded_customer_count: Math.max(0, features.length - prepCustomers.length),
					manually_excluded_client_nums: Array.from(manualExcludedClientNums).sort((a, b) => a - b),
					manually_included_client_nums: Array.from(manualIncludedClientNums).sort((a, b) => a - b),
					included_customer_ids: prepCustomers.map((customer) => customer.customer_id).filter(Boolean),
					included_customers: prepCustomers.map((customer) => ({
						client_num: customer.client_num,
						customer_id: customer.customer_id,
						customer_name: customer.customer_name
					})),
					clustering: {
						method: clusteringMethod,
						method_label: demandAwareClustering ? `${clusteringMethod}_demand_aware` : clusteringMethod,
						demand_aware: demandAwareClustering,
						cluster_count: clusterCountValue,
						target_cluster_size: clusterSize,
						seed: previewSeed,
						tree_height: previewHeight,
						iterations: previewIteration,
						converged: previewConverged,
						confirmed_at: new Date().toISOString()
					}
				};
			await persistInstanceRun({
				targetRunId,
				sourceRunId: runId,
				prepCustomers,
				demandRows: filteredDemandRows,
				mapFeatures,
				assignmentRows: displayedAssignments,
				warehouseLocation: warehouse,
				clusteringMethodValue,
				maxDistanceFromWarehouseKm: activeRadiusKm,
				instanceSetupPayload
			});
			createStatus = `Created ${targetRunId}.`;
			appState.refreshNow();
			} catch (error) {
				createStatus = error instanceof Error ? error.message : String(error);
			} finally {
				isCreating = false;
			}
		}

	async function importInstanceBundle(selectedFile: File | null = bundleFile): Promise<void> {
		const file = selectedFile ?? bundleFile;
		if (!file) {
			importStatus = 'Choose a bundle file.';
			return;
		}

		isImportingBundle = true;
		importStatus = 'Importing bundle...';

		try {
			const bundleText = await file.text();
			const { bundle, prepCustomers, demandRows, warehouseLocation } = parseInstanceBundle(bundleText);
			const assignmentRows = buildAssignmentsFromPrepCustomers(prepCustomers);
			const bundleRunId = sanitizeRunId(bundle.run_id ?? '');
			const fileRunId = sanitizeRunId(file.name.replace(/\.json$/i, ''));
			const fallbackRunId = nextAvailableRunId(buildDatedRunIdBase());
			const requestedRunId = bundleRunId || fileRunId || fallbackRunId;
			const targetRunId = resolveUniqueRunId(requestedRunId);
			if (!targetRunId) {
				importStatus = 'Bundle needs an instance id or filename.';
				return;
			}

			const mapFeatures = buildFeaturesFromPrepCustomers(prepCustomers);
			const uniqueClusters = new Set(assignmentRows.map((row) => row.cluster_id)).size;
			await persistInstanceRun({
				targetRunId,
				sourceRunId: bundle.source_run_id ?? null,
				prepCustomers,
				demandRows,
				mapFeatures,
				assignmentRows,
				warehouseLocation,
				clusteringMethodValue: bundle.clustering_method ?? 'imported_bundle',
				maxDistanceFromWarehouseKm:
					typeof bundle.max_distance_from_warehouse_km === 'number' ? bundle.max_distance_from_warehouse_km : null,
				instanceSetupPayload: {
					source_run_id: bundle.source_run_id ?? null,
					import_mode: 'bundle',
					import_bundle_name: file.name,
					generated_at: bundle.generated_at ?? null,
					warehouse: warehouseLocation,
					included_customer_count: prepCustomers.length,
					excluded_customer_count: 0,
					manually_excluded_client_nums: [],
					included_customer_ids: prepCustomers.map((customer) => customer.customer_id).filter(Boolean),
					included_customers: prepCustomers.map((customer) => ({
						client_num: customer.client_num,
						customer_id: customer.customer_id,
						customer_name: customer.customer_name
					})),
					clustering: {
						method: bundle.clustering_method ?? 'imported_bundle',
						method_label: bundle.clustering_method ?? 'imported_bundle',
						demand_aware: null,
						cluster_count: uniqueClusters,
						target_cluster_size: null,
						seed: null,
						tree_height: null,
						iterations: null,
						converged: null,
						confirmed_at: bundle.generated_at ?? null
					}
				},
				navigateTo: `/instances/new?step=customers`
			});
		} catch (error) {
			importStatus = error instanceof Error ? error.message : String(error);
		} finally {
			isImportingBundle = false;
		}
	}
</script>

<section class="panel stack">
	<div class="section-title">
		<h1>New Instance</h1>
		<a class="link-button" href={runId ? `/instances/${runId}` : '/instances'}>Back To Instances</a>
	</div>

	<div class="stepper">
		{#each workflowSteps as step, index}
			<button
				type="button"
				class:active={activeStep === step}
				class:locked={(step === 'customers' && sourceMode !== 'create_existing' && sourceMode !== 'upload') || (step === 'clustering' && ((sourceMode !== 'create_existing' && sourceMode !== 'upload') || includedCustomerCount < 1))}
				onclick={() => goToStep(step)}
			>
				<span>{index + 1}</span>
				<strong>{step === 'source' ? 'Source' : step === 'customers' ? 'Filtering' : 'Clustering'}</strong>
			</button>
		{/each}
	</div>
</section>

{#if activeStep === 'source'}
	<section class="panel stack">
		<div class="source-minimal">
			<div class="source-column">
				<div class="source-label">Select From Existing</div>
				{#if availableInstances.length}
					<div class="instance-list" role="list">
						{#each availableInstances as instanceName}
							<div role="listitem">
								<button
									type="button"
									class="instance-option"
									class:selected={instanceName === runId}
									aria-current={instanceName === runId ? 'true' : undefined}
									onclick={() => void openSourceRun(instanceName)}
								>
									<span class="instance-name">{instanceName}</span>
									{#if instanceName === runId}
										<strong class="instance-badge">Selected</strong>
									{/if}
								</button>
							</div>
						{/each}
					</div>
				{:else}
					<div class="empty-state">No instance available.</div>
				{/if}
			</div>

			<div class="source-column upload-column">
				<div class="source-label">Upload</div>
				<input
					class="upload-input"
					type="file"
					accept=".json,application/json"
					onchange={(event) => void handleBundleFileChange(event)}
					disabled={isImportingBundle}
				/>
				{#if importStatus}
					<div class="inline-message">{importStatus}</div>
				{/if}
			</div>
		</div>
	</section>
{/if}

{#if activeStep === 'customers'}
	<section class="panel stack">
		<div class="step-header">
			<div>
				<h2>Filtering</h2>
			</div>
		</div>

		<div class="workspace-controls filtering-controls">
			<label class="control toggle-control">
				<span>Radius Filter</span>
				<input type="checkbox" checked={useRadius} onchange={(event) => setRadiusEnabled((event.currentTarget as HTMLInputElement).checked)} disabled={!warehouse} />
			</label>

			{#if useRadius}
				<label class="control radius-control">
					<span>Radius</span>
					<input type="range" min="1" max={maxRadiusKm} step="1" value={selectionRadiusKm} oninput={(event) => updateRadius(Number((event.currentTarget as HTMLInputElement).value))} />
					<input class="number-input" type="number" min="1" max={maxRadiusKm} step="1" value={selectionRadiusKm} onchange={(event) => updateRadius(Number((event.currentTarget as HTMLInputElement).value))} />
				</label>
			{/if}

			<button type="button" onclick={resetSelection}>Reset Selection</button>
		</div>

		<div class="workspace-layout">
			<div class="map-column">
				<SimpleMap
					data={mapData}
					summary={summary}
					demandRows={mapDemandRows}
					allowedModes={['total']}
					defaultMode="total"
					selectionRadiusKm={activeRadiusKm}
					includedClientNums={includedClientNums}
					onCustomerClick={toggleCustomer}
					height={500}
				/>
			</div>

			<aside class="info-rail">
				<div class="rail-section">
					<div class="rail-title">Selection</div>
					<div class="kv"><span>Selected</span><strong>{formatNumber(includedCustomerCount, 0)}</strong></div>
					<div class="kv"><span>Excluded</span><strong>{formatNumber(excludedCustomerCount, 0)}</strong></div>
					<div class="kv"><span>Demand</span><strong>{formatNumber(includedDemandKg, 0)} kg</strong></div>
					<div class="kv"><span>Share</span><strong>{formatNumber(includedShare, 1)}%</strong></div>
				</div>

				<div class="rail-section">
					<div class="rail-title">Filters</div>
					<div class="kv"><span>Radius</span><strong>{useRadius ? `${formatNumber(selectionRadiusKm, 0)} km` : 'off'}</strong></div>
					<div class="kv"><span>Warehouse</span><strong>{warehouse ? 'available' : 'missing'}</strong></div>
					<div class="kv"><span>Manual Off</span><strong>{formatNumber(manualExcludedClientNums.size, 0)}</strong></div>
					<div class="kv"><span>Manual On</span><strong>{formatNumber(manualIncludedClientNums.size, 0)}</strong></div>
				</div>
			</aside>
		</div>

		<div class="step-actions">
			<button type="button" onclick={() => goToStep('source')}>Back</button>
			<button class="primary-action" type="button" onclick={() => goToStep('clustering')} disabled={includedCustomerCount < 1}>Continue To Clustering</button>
		</div>
	</section>
{/if}

{#if activeStep === 'clustering'}
	<section class="panel stack">
		<div class="step-header">
			<div><h2>Clustering Setup</h2></div>
		</div>

		<div class="workspace-controls clustering-controls">
			<label class="control runid-control">
				<span>New Instance ID</span>
				<input bind:value={newRunIdInput} placeholder={suggestedRunId || 'new_instance_run'} />
			</label>

			<label class="control compact-control">
				<span>Method</span>
				<select bind:value={clusteringMethod}>
					<option value="angular_gap">Angular Slices</option>
					<option value="kmeans">K-Means</option>
					<!-- <option value="depot_regularized_kmeans">Depot-Regularized K-Means</option> -->
					<option value="hierarchical_ward">Hierarchical Ward</option>
				</select>
			</label>

			<label class="control compact-control">
				<span>Cluster Size</span>
				<input type="number" min="1" max={Math.max(1, selectedCustomers.length)} bind:value={clusterSize} />
			</label>

			<label
				class="control compact-control"
				style={!methodSupportsDemandAware(clusteringMethod) ? 'opacity: 0.5; pointer-events: none;' : undefined}
			>
				<span>Demand-Aware</span>
				<input
					type="checkbox"
					bind:checked={demandAwareClustering}
					disabled={!methodSupportsDemandAware(clusteringMethod)}
				/>
			</label>

			{#if clusteringMethod === 'depot_regularized_kmeans'}
				<label class="control compact-control">
					<span>Depot Weight</span>
					<input type="number" min="0" step="0.5" bind:value={depotRegularizationLambda} />
				</label>
			{/if}
			<div class="control play-control">
				<span>Preview</span>
				<button
					class="play-button"
					type="button"
					aria-label={displayedAssignments.length ? 'Run clustering again' : 'Run clustering'}
					title={displayedAssignments.length ? 'Run clustering again' : 'Run clustering'}
					onclick={() => void runClusteringPreview()}
					disabled={isRunningClustering || !selectedCustomers.length}
				>
					{#if isRunningClustering}
						…
					{:else}
						&#9654;
					{/if}
				</button>
			</div>
		</div>

		<div class="workspace-layout">
			<div class="map-column">
				<SimpleMap
					data={selectedMapData}
					summary={summary}
					demandRows={filteredDemandRows}
					allowedModes={displayedAssignments.length ? ['clusters'] : ['customers']}
					defaultMode={displayedAssignments.length ? 'clusters' : 'customers'}
					clusterAssignments={displayedAssignments.length ? clusteringStepClusterMap : null}
					clusterCentroids={displayedAssignments.length ? displayedCentroids : []}
					showClusterCentroids={clusteringMethod === 'kmeans'}
					sliceBoundaries={displayedAngularSliceBoundaries}
					height={500}
				/>
			</div>

			<aside class="info-rail">
				<div class="rail-section">
					<div class="rail-title">Selection</div>
					<div class="kv"><span>Stores</span><strong>{formatNumber(includedCustomerCount, 0)}</strong></div>
					<div class="kv"><span>Cluster Size</span><strong>{formatNumber(clusterSize, 0)}</strong></div>
					<div class="kv"><span>Clusters</span><strong>{formatNumber(suggestedClusterCount, 0)}</strong></div>
					<div class="kv"><span>Preview</span><strong>{formatNumber(previewClusterCount, 0)}</strong></div>
					<div class="kv"><span>Radius</span><strong>{useRadius ? `${formatNumber(selectionRadiusKm, 0)} km` : 'off'}</strong></div>
				</div>

				<div class="rail-section">
					<div class="rail-title">Preview</div>
					<div class="kv"><span>Method</span><strong>{previewMethod ?? 'pending'}</strong></div>
					<div class="kv"><span>Steps</span><strong>{formatNumber(previewTotalSteps, 0)}</strong></div>
					<div class="kv"><span>Iterations</span><strong>{formatNumber(previewIteration, 0)}</strong></div>
					<div class="kv"><span>Converged</span><strong>{previewConverged ? 'yes' : 'no'}</strong></div>
					<div class="kv">
						<span>{previewMethod === 'hierarchical_ward' ? 'Tree' : previewMethod === 'angular_gap' ? 'Gap' : previewMethod === 'depot_regularized_kmeans' ? 'Lambda' : 'Seed'}</span>
						<strong>
							{previewMethod === 'hierarchical_ward'
								? (previewHeight !== null ? formatNumber(previewHeight, 3) : 'n/a')
								: previewMethod === 'angular_gap'
									? (previewLargestGap !== null ? `${formatNumber((previewLargestGap * 180) / Math.PI, 1)} deg` : 'n/a')
									: previewMethod === 'depot_regularized_kmeans'
										? formatNumber(depotRegularizationLambda, 2)
										: (previewSeed ?? 'n/a')}
						</strong>
					</div>
				</div>

				{#if previewClusterSummary}
					<div class="rail-section">
						<div class="rail-title">Cluster Summary</div>
						<div class="kv"><span>Clusters</span><strong>{formatNumber(previewClusterSummary.clusterCount, 0)}</strong></div>
						<div class="kv">
							<span>Stores / Cluster</span>
							<strong>{formatNumber(previewClusterSummary.minCustomers, 0)} - {formatNumber(previewClusterSummary.maxCustomers, 0)}</strong>
						</div>
						<div class="kv">
							<span>Demand kg / Cluster</span>
							<strong>{formatNumber(previewClusterSummary.minDemandKg, 0)} - {formatNumber(previewClusterSummary.maxDemandKg, 0)}</strong>
						</div>
					</div>
				{/if}
			</aside>
		</div>

		<div class="step-actions">
			<button type="button" onclick={() => goToStep('customers')}>Back</button>
			<button class="primary-action" type="button" onclick={() => void createClusteredInstance()} disabled={!previewIsFresh || isRunningClustering || isCreating}>
				{isCreating ? 'Creating...' : 'Create Instance'}
			</button>
			{#if createStatus}
				<span class="status-text">{createStatus}</span>
			{/if}
		</div>
	</section>
{/if}

<style>
	.stepper {
		display: grid;
		grid-template-columns: repeat(3, minmax(0, 1fr));
		gap: 0.75rem;
	}

	.stepper button,
	.workspace-controls input,
	.workspace-controls select,
	.workspace-controls button {
		border: 1px solid var(--border);
		border-radius: 0;
	}

	.stepper button {
		display: flex;
		align-items: center;
		gap: 0.5rem;
		padding: 0.75rem 0.85rem;
		text-align: left;
		border-radius: 0;
	}

	.stepper button span {
		font-size: 0.78rem;
		color: var(--muted);
		line-height: 1;
	}

	.stepper button.active {
		border-color: #7aa2e3;
		color: #184a97;
	}

	.stepper button.locked {
		opacity: 0.55;
		pointer-events: none;
		cursor: not-allowed;
	}

	.source-minimal {
		display: grid;
		gap: 1.6rem;
		align-items: start;
	}

	.source-column {
		display: grid;
		gap: 0.6rem;
	}

	.instance-list {
		margin: 0;
		display: grid;
		gap: 0.5rem;
	}

	.source-label {
		font-size: 0.78rem;
		font-weight: 700;
		letter-spacing: 0.02em;
		text-transform: uppercase;
		color: var(--muted);
	}

	.instance-option {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 0.75rem;
		width: 100%;
		padding: 0.65rem 0.8rem;
		border: 1px solid var(--border);
		text-align: left;
		border-radius: 0;
		transition:
			border-color 120ms ease,
			color 120ms ease;
	}

	.instance-option:hover {
		border-color: var(--text);
	}

	.instance-option.selected {
		border-color: #5f86c8;
		box-shadow: inset 3px 0 0 #5f86c8;
	}

	.instance-name {
		flex: 1;
		min-width: 0;
		font-weight: 600;
		text-align: left;
	}

	.instance-badge {
		flex: 0 0 auto;
		padding: 0.18rem 0.45rem;
		border: 1px solid #5f86c8;
		background: transparent;
		font-size: 0.74rem;
		letter-spacing: 0.02em;
		text-transform: uppercase;
		color: #2f5da8;
	}

	.upload-column {
		align-content: start;
	}

	.upload-input {
		width: 100%;
		border: 1px solid var(--border);
		border-radius: 0;
		padding: 0.45rem;
		background: transparent;
	}

	.empty-state,
	.inline-message {
		font-size: 0.9rem;
		color: var(--muted);
	}

	.step-header {
		display: grid;
		gap: 1rem;
	}

	.control {
		display: grid;
		gap: 0.35rem;
	}

	.control span {
		font-size: 0.78rem;
		font-weight: 600;
		color: var(--muted);
	}

	.workspace-controls {
		display: flex;
		flex-wrap: wrap;
		gap: 0.75rem;
		align-items: end;
	}

	.workspace-controls input,
	.workspace-controls select,
	.workspace-controls button {
		padding: 0.6rem 0.7rem;
		border-radius: 0;
	}

	.filtering-controls {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(14rem, max-content));
		gap: 1rem 1.25rem;
		align-items: end;
	}

	.clustering-controls {
		display: grid;
		grid-template-columns: minmax(16rem, 1.6fr) repeat(3, minmax(10rem, 1fr)) minmax(5rem, auto);
		gap: 1rem;
		align-items: end;
		padding: 1rem;
		border: 1px solid var(--border);
	}

	.toggle-control {
		min-width: 9rem;
	}

	.toggle-control input {
		width: 1rem;
		height: 1rem;
		padding: 0;
	}

	.radius-control {
		grid-template-columns: minmax(12rem, 18rem) 6.5rem;
		align-items: center;
		column-gap: 0.6rem;
	}

	.radius-control span {
		grid-column: 1 / -1;
	}

	.runid-control {
		min-width: 0;
	}

	.compact-control {
		min-width: 0;
	}

	.number-input {
		width: 100%;
	}

	.play-control {
		min-width: 0;
		justify-self: end;
		align-self: stretch;
	}

	.primary-action {
		font-weight: 600;
	}

	.play-button {
		min-width: 3.5rem;
		height: 100%;
		padding-inline: 0.9rem;
		font-size: 1.1rem;
		font-weight: 700;
		line-height: 1;
		background: #f4efe4;
	}

	.play-button:disabled {
		opacity: 0.6;
	}

	.workspace-layout {
		display: grid;
		grid-template-columns: minmax(0, 1fr) 16rem;
		gap: 1rem;
		align-items: start;
	}

	.map-column {
		min-width: 0;
		--map-header-offset: 0;
	}

	.info-rail {
		display: grid;
		gap: 0.8rem;
		align-self: start;
		margin-top: var(--map-header-offset);
	}

	.rail-section {
		border: 1px solid var(--border);
		padding: 0.85rem 0.9rem;
		display: grid;
		gap: 0.55rem;
	}

	.rail-title {
		font-size: 0.78rem;
		font-weight: 700;
		letter-spacing: 0.02em;
		text-transform: uppercase;
		color: var(--muted);
	}

	.kv {
		display: flex;
		justify-content: space-between;
		gap: 0.75rem;
		font-size: 0.9rem;
	}

	.kv span {
		color: var(--muted);
	}

	.kv strong {
		font-variant-numeric: tabular-nums;
	}

	.step-actions {
		display: flex;
		justify-content: flex-start;
		gap: 0.75rem;
		flex-wrap: wrap;
	}

	.step-actions .primary-action {
		margin-left: auto;
	}

	.step-actions .status-text {
		flex-basis: 100%;
		font-size: 0.85rem;
		color: var(--muted);
	}

	@media (max-width: 760px) {
		.stepper,
		.source-minimal,
		.workspace-layout,
		.radius-control,
		.step-actions {
			grid-template-columns: 1fr;
		}

		.workspace-layout {
			display: grid;
		}

		.info-rail {
			margin-top: 0;
		}

		.workspace-controls,
		.step-actions {
			flex-direction: column;
			align-items: stretch;
		}

		.filtering-controls {
			display: grid;
			grid-template-columns: 1fr;
		}

		.clustering-controls {
			grid-template-columns: 1fr;
		}

		.play-control {
			justify-self: stretch;
		}

		.step-actions .primary-action {
			margin-left: 0;
		}
	}
</style>
