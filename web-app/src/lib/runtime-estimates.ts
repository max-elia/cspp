import type { PipelineRuntimeEstimate } from '$lib/types';

type ClusterRuntimeBucket = 'tiny' | 'small' | 'compact_light' | 'medium' | 'hard' | 'hard_demand';

type ClusterStats = {
	clusterId: number;
	customers: number;
	avgDepotKm: number | null;
	meanActiveCustomers: number | null;
	meanTotalDemandKg: number | null;
	p90TotalDemandKg: number | null;
};

const STAGE1_TIMELIMITS: Record<ClusterRuntimeBucket, number> = {
	tiny: 120,
	small: 180,
	compact_light: 240,
	medium: 300,
	hard: 360,
	hard_demand: 450
};

const STAGE2_TIMELIMITS: Record<ClusterRuntimeBucket, number> = {
	tiny: 45,
	small: 60,
	compact_light: 120,
	medium: 180,
	hard: 240,
	hard_demand: 300
};

const DEMAND_PROMOTION_MEAN_TOTAL_THRESHOLD_KG = 105_000;
const DEMAND_PROMOTION_P90_TOTAL_THRESHOLD_KG = 145_000;

const STAGE1_CONTINUATION_MULTIPLIER: Record<ClusterRuntimeBucket, number> = {
	tiny: 1.0,
	small: 1.05,
	compact_light: 1.15,
	medium: 1.3,
	hard: 1.45,
	hard_demand: 1.6
};

const STAGE2_CONTINUATION_MULTIPLIER: Record<ClusterRuntimeBucket, number> = {
	tiny: 1.0,
	small: 1.0,
	compact_light: 1.1,
	medium: 1.2,
	hard: 1.35,
	hard_demand: 1.5
};

function asNumber(value: unknown): number | null {
	const parsed = Number(value);
	return Number.isFinite(parsed) ? parsed : null;
}

function quantile(values: number[], p: number): number | null {
	if (!values.length) return null;
	const sorted = [...values].sort((left, right) => left - right);
	const index = (sorted.length - 1) * p;
	const lower = Math.floor(index);
	const upper = Math.ceil(index);
	if (lower === upper) return sorted[lower];
	return sorted[lower] + (sorted[upper] - sorted[lower]) * (index - lower);
}

function haversineKm(latA: number, lonA: number, latB: number, lonB: number): number {
	const toRad = (degrees: number) => (degrees * Math.PI) / 180;
	const dLat = toRad(latB - latA);
	const dLon = toRad(lonB - lonA);
	const a =
		Math.sin(dLat / 2) ** 2 +
		Math.cos(toRad(latA)) * Math.cos(toRad(latB)) * Math.sin(dLon / 2) ** 2;
	return 2 * 6371 * Math.asin(Math.sqrt(a));
}

function classifyBaseBucket(stats: ClusterStats): Exclude<ClusterRuntimeBucket, 'hard_demand'> {
	const { customers, avgDepotKm, meanActiveCustomers } = stats;
	if (customers <= 12) return 'tiny';
	if (customers <= 14) return 'small';
	if (
		customers <= 16 &&
		avgDepotKm !== null &&
		meanActiveCustomers !== null &&
		avgDepotKm < 15 &&
		meanActiveCustomers < 8.5
	) {
		return 'compact_light';
	}
	if (
		customers <= 20 &&
		avgDepotKm !== null &&
		meanActiveCustomers !== null &&
		avgDepotKm < 30 &&
		meanActiveCustomers < 10
	) {
		return 'medium';
	}
	return 'hard';
}

function classifyBucket(stats: ClusterStats): ClusterRuntimeBucket {
	const baseBucket = classifyBaseBucket(stats);
	if (baseBucket !== 'medium' && baseBucket !== 'hard') return baseBucket;
	const demandPromotion =
		(stats.meanTotalDemandKg !== null && stats.meanTotalDemandKg >= DEMAND_PROMOTION_MEAN_TOTAL_THRESHOLD_KG) ||
		(stats.p90TotalDemandKg !== null && stats.p90TotalDemandKg >= DEMAND_PROMOTION_P90_TOTAL_THRESHOLD_KG);
	if (!demandPromotion) return baseBucket;
	return baseBucket === 'medium' ? 'hard' : 'hard_demand';
}

function computeClusterStats(instancePayload: Record<string, unknown>): ClusterStats[] {
	const customers = Array.isArray(instancePayload.stores)
		? instancePayload.stores
		: Array.isArray(instancePayload.customers)
			? instancePayload.customers
			: [];
	const demandRows = Array.isArray(instancePayload.demand_rows) ? instancePayload.demand_rows : [];
	const warehouse =
		instancePayload.warehouse && typeof instancePayload.warehouse === 'object'
			? (instancePayload.warehouse as Record<string, unknown>)
			: null;

	const warehouseLat = asNumber(warehouse?.latitude);
	const warehouseLon = asNumber(warehouse?.longitude);

	const clusterCustomers = new Map<number, Array<Record<string, unknown>>>();
	const customerClusterByClientNum = new Map<number, number>();
	for (const customer of customers) {
		if (!customer || typeof customer !== 'object') continue;
		const row = customer as Record<string, unknown>;
		const clusterId = asNumber(row.cluster_id);
		if (clusterId === null) continue;
		const clientNum = asNumber(row.client_num);
		if (clientNum !== null) customerClusterByClientNum.set(clientNum, clusterId);
		const clusterRows = clusterCustomers.get(clusterId) ?? [];
		clusterRows.push(row);
		clusterCustomers.set(clusterId, clusterRows);
	}

	const clusterDemandByDate = new Map<number, Map<string, { totalDemandKg: number; activeCustomers: Set<number> }>>();
	for (const row of demandRows) {
		if (!row || typeof row !== 'object') continue;
		const demandRow = row as Record<string, unknown>;
		const clientNum = asNumber(demandRow.client_num);
		const demandKg = asNumber(demandRow.demand_kg);
		const deliveryDate = String(demandRow.delivery_date ?? '').trim();
		if (clientNum === null || demandKg === null || !deliveryDate) continue;
		const clusterId = customerClusterByClientNum.get(clientNum) ?? null;
		if (clusterId === null) continue;
		const byDate = clusterDemandByDate.get(clusterId) ?? new Map<string, { totalDemandKg: number; activeCustomers: Set<number> }>();
		const current = byDate.get(deliveryDate) ?? { totalDemandKg: 0, activeCustomers: new Set<number>() };
		current.totalDemandKg += demandKg;
		if (demandKg > 0) current.activeCustomers.add(clientNum);
		byDate.set(deliveryDate, current);
		clusterDemandByDate.set(clusterId, byDate);
	}

	const stats: ClusterStats[] = [];
	for (const [clusterId, rows] of clusterCustomers.entries()) {
		const depotDistances: number[] = [];
		for (const row of rows) {
			const latitude = asNumber(row.latitude);
			const longitude = asNumber(row.longitude);
			if (warehouseLat === null || warehouseLon === null || latitude === null || longitude === null) continue;
			depotDistances.push(1.25 * haversineKm(warehouseLat, warehouseLon, latitude, longitude));
		}
		const byDate = clusterDemandByDate.get(clusterId);
		const scenarioTotals = byDate ? [...byDate.values()].map((value) => value.totalDemandKg) : [];
		const activeCounts = byDate ? [...byDate.values()].map((value) => value.activeCustomers.size) : [];
		stats.push({
			clusterId,
			customers: rows.length,
			avgDepotKm: depotDistances.length ? depotDistances.reduce((sum, value) => sum + value, 0) / depotDistances.length : null,
			meanActiveCustomers: activeCounts.length
				? activeCounts.reduce((sum, value) => sum + value, 0) / activeCounts.length
				: null,
			meanTotalDemandKg: scenarioTotals.length
				? scenarioTotals.reduce((sum, value) => sum + value, 0) / scenarioTotals.length
				: null,
			p90TotalDemandKg: quantile(scenarioTotals, 0.9)
		});
	}
	return stats.sort((left, right) => left.clusterId - right.clusterId);
}

function averageStage1RuntimePerClusterSec(
	clusterStats: ClusterStats[],
	stage1MaxIterations: number,
	totalScenarios: number
): number {
	if (!clusterStats.length) return 0;
	return (
		clusterStats.reduce((sum, stats) => {
			const bucket = classifyBucket(stats);
			const baseLimit = STAGE1_TIMELIMITS[bucket];
			const continuation = STAGE1_CONTINUATION_MULTIPLIER[bucket];
			const expectedIterations = Math.max(1, Math.min(stage1MaxIterations, Math.ceil(totalScenarios / 3)));
			return sum + baseLimit * continuation * expectedIterations;
		}, 0) / clusterStats.length
	);
}

function averageStage2RuntimePerCaseSec(clusterStats: ClusterStats[]): number {
	if (!clusterStats.length) return 0;
	return (
		clusterStats.reduce((sum, stats) => {
			const bucket = classifyBucket(stats);
			return sum + STAGE2_TIMELIMITS[bucket] * STAGE2_CONTINUATION_MULTIPLIER[bucket];
		}, 0) / clusterStats.length
	);
}

function clusterSolveSlots(selectedCores: number, clusterCount: number): number {
	return Math.max(1, Math.min(selectedCores, clusterCount));
}

export function estimatePipelineRuntimeFallbackSec(
	instancePayload: Record<string, unknown> | null,
	runConfig: Record<string, unknown> | null,
	selectedCores: number | null
): number | null {
	if (!instancePayload || !selectedCores || selectedCores <= 0) return null;

	const customers = Array.isArray(instancePayload.stores)
		? instancePayload.stores
		: Array.isArray(instancePayload.customers)
			? instancePayload.customers
			: [];
	const demandRows = Array.isArray(instancePayload.demand_rows) ? instancePayload.demand_rows : [];
	if (!customers.length || !demandRows.length) return null;

	const clusterStats = computeClusterStats(instancePayload);
	const clusterCount = clusterStats.length;
	const totalCustomers = customers.length;
	const scenarioDates = new Set<string>();
	for (const row of demandRows) {
		if (!row || typeof row !== 'object') continue;
		const deliveryDate = String((row as Record<string, unknown>).delivery_date ?? '').trim();
		if (deliveryDate) scenarioDates.add(deliveryDate);
	}
	const totalScenarios = Math.max(1, scenarioDates.size);
	if (clusterCount < 1) return null;

	const stage1MaxIterations = Math.max(1, Math.round(asNumber(runConfig?.stage1_max_iterations) ?? 6));
	const reoptMaxIterations = Math.max(1, Math.round(asNumber(runConfig?.reopt_max_iterations) ?? totalScenarios));

	const stage1PerClusterSec = averageStage1RuntimePerClusterSec(clusterStats, stage1MaxIterations, totalScenarios);
	const stage1Sec = (clusterCount * stage1PerClusterSec) / clusterSolveSlots(selectedCores, clusterCount);

	const stage2PerCaseSec = averageStage2RuntimePerCaseSec(clusterStats);
	const stage2Runs = clusterCount * totalScenarios;
	const stage2Sec = (stage2Runs * stage2PerCaseSec) / clusterSolveSlots(selectedCores, stage2Runs);

	const stage3ClusterSolveSec =
		(clusterCount * stage1PerClusterSec) / clusterSolveSlots(selectedCores, clusterCount);
	const stage3EvalRuns = clusterCount * totalScenarios;
	const stage3EvalSec =
		(2 * stage3EvalRuns * stage2PerCaseSec) / clusterSolveSlots(selectedCores, stage3EvalRuns);
	const stage3Sec = reoptMaxIterations * (stage3ClusterSolveSec + stage3EvalSec);

	return stage1Sec + stage2Sec + stage3Sec;
}

export function estimatePipelineRuntimeForSelectionSec(
	pipelineEstimate: PipelineRuntimeEstimate | null | undefined,
	fallbackEstimateTotalSec: number | null,
	selectedCores: number | null
): number | null {
	const baseTotal = pipelineEstimate?.estimated_total_sec ?? null;
	const baseCores = pipelineEstimate?.usable_cores ?? null;
	if (baseTotal == null) return fallbackEstimateTotalSec;
	if (selectedCores == null || !Number.isFinite(selectedCores) || selectedCores <= 0) return baseTotal;
	if (baseCores == null || !Number.isFinite(baseCores) || baseCores <= 0) return baseTotal;
	return (baseTotal * baseCores) / selectedCores;
}
