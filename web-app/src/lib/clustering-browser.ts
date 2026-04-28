import { agnes, type AgglomerationMethod, type Cluster } from 'ml-hclust';

export type CustomerRecord = {
	client_num: number;
	customer_id?: string | null;
	customer_name?: string | null;
	latitude: number;
	longitude: number;
	cluster_id?: number | null;
	super_cluster_id?: number | null;
	total_demand_kg?: number | null;
};

export type AssignmentRow = {
	client_num: number;
	customer_id?: string;
	cluster_id: number;
	super_cluster_id: number;
};

export type ClusterCentroid = {
	cluster_id: number;
	latitude: number;
	longitude: number;
	size: number;
};

export type ClusterPreviewStep = {
	iteration: number;
	assignments: AssignmentRow[];
	centroids: ClusterCentroid[];
	converged: boolean;
};

export type BrowserKMeansPreview = {
	seed: number;
	steps: ClusterPreviewStep[];
	finalAssignments: AssignmentRow[];
	finalCentroids: ClusterCentroid[];
	iterations: number;
	converged: boolean;
};

export type BrowserHierarchicalPreview = {
	linkage: AgglomerationMethod;
	steps: ClusterPreviewStep[];
	finalAssignments: AssignmentRow[];
	finalCentroids: ClusterCentroid[];
	iterations: number;
	converged: boolean;
	finalHeight: number;
};

export type BrowserAngularGapPreview = {
	steps: ClusterPreviewStep[];
	finalAssignments: AssignmentRow[];
	finalCentroids: ClusterCentroid[];
	iterations: number;
	converged: boolean;
	selectedGapSizes: number[];
};

type Point2D = [number, number];

type SeededRandom = () => number;

type KMeansOptions = {
	maxIterations?: number;
	seed?: number;
	demandAware?: boolean;
	depot?: { latitude: number; longitude: number } | null;
	lambda?: number;
};

export function chooseClusterCount(nCustomers: number, targetSize = 16): number {
	if (nCustomers <= 0) return 0;
	return Math.max(1, Math.min(nCustomers, Math.round(nCustomers / targetSize) || 1));
}

function assignmentsFromClusters(customers: CustomerRecord[], clusters: number[]): AssignmentRow[] {
	return customers.map((customer, index) => ({
		client_num: customer.client_num,
		customer_id: customer.customer_id ?? undefined,
		cluster_id: clusters[index] ?? 0,
		super_cluster_id: 0
	}));
}

function centroidsFromAssignments(customers: CustomerRecord[], rows: AssignmentRow[]): ClusterCentroid[] {
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

function centroidsFromResult(centroids: number[][], clusters: number[]): ClusterCentroid[] {
	const counts = new Map<number, number>();
	for (const clusterId of clusters) {
		counts.set(clusterId, (counts.get(clusterId) ?? 0) + 1);
	}
	return centroids.map((centroid, clusterId) => ({
		cluster_id: clusterId,
		latitude: centroid[0] ?? 0,
		longitude: centroid[1] ?? 0,
		size: counts.get(clusterId) ?? 0
	}));
}

function createSeededRandom(seed: number): SeededRandom {
	let state = seed % 2_147_483_647;
	if (state <= 0) state += 2_147_483_646;
	return () => {
		state = (state * 16_807) % 2_147_483_647;
		return (state - 1) / 2_147_483_646;
	};
}

function customerWeights(customers: CustomerRecord[], demandAware = false): number[] {
	if (!demandAware) return new Array(customers.length).fill(1);
	const raw = customers.map((customer) => {
		const demand = Number(customer.total_demand_kg);
		return Number.isFinite(demand) && demand > 0 ? demand : 0;
	});
	return raw.some((value) => value > 0) ? raw : new Array(customers.length).fill(1);
}

function balancedClusterCapacities(totalCustomers: number, clusterCount: number): number[] {
	if (clusterCount <= 0) return [];
	const baseSize = Math.floor(totalCustomers / clusterCount);
	const remainder = totalCustomers % clusterCount;
	return new Array(clusterCount).fill(baseSize).map((size, index) => size + (index < remainder ? 1 : 0));
}

function squaredDistance(a: Point2D, b: Point2D): number {
	const dLat = a[0] - b[0];
	const dLon = a[1] - b[1];
	return dLat * dLat + dLon * dLon;
}

function assignmentScores(points: Point2D[], centroids: Point2D[], depotPoint: Point2D | null, lambda: number): number[][] {
	return points.map((point) =>
		centroids.map((centroid) => {
			const depotPenalty = depotPoint ? lambda * squaredDistance(centroid, depotPoint) : 0;
			return squaredDistance(point, centroid) + depotPenalty;
		})
	);
}

function nearestClusterAssignments(scores: number[][]): number[] {
	return scores.map((pointScores) => {
		let bestCluster = 0;
		let bestScore = Number.POSITIVE_INFINITY;
		for (let clusterId = 0; clusterId < pointScores.length; clusterId += 1) {
			const score = pointScores[clusterId] ?? Number.POSITIVE_INFINITY;
			if (score < bestScore) {
				bestScore = score;
				bestCluster = clusterId;
			}
		}
		return bestCluster;
	});
}

function rebalanceAssignmentsToCapacities(scores: number[][], capacities: number[]): number[] {
	const assignments = nearestClusterAssignments(scores);
	const counts = new Array(capacities.length).fill(0);
	for (const clusterId of assignments) {
		counts[clusterId] = (counts[clusterId] ?? 0) + 1;
	}

	while (true) {
		let sourceCluster = -1;
		for (let clusterId = 0; clusterId < capacities.length; clusterId += 1) {
			if ((counts[clusterId] ?? 0) > (capacities[clusterId] ?? 0)) {
				sourceCluster = clusterId;
				break;
			}
		}
		if (sourceCluster < 0) break;

		let bestPointIndex = -1;
		let bestTargetCluster = -1;
		let bestDelta = Number.POSITIVE_INFINITY;

		for (let pointIndex = 0; pointIndex < assignments.length; pointIndex += 1) {
			if (assignments[pointIndex] !== sourceCluster) continue;
			const sourceScore = scores[pointIndex]?.[sourceCluster] ?? Number.POSITIVE_INFINITY;
			for (let targetCluster = 0; targetCluster < capacities.length; targetCluster += 1) {
				if ((counts[targetCluster] ?? 0) >= (capacities[targetCluster] ?? 0)) continue;
				const targetScore = scores[pointIndex]?.[targetCluster] ?? Number.POSITIVE_INFINITY;
				const delta = targetScore - sourceScore;
				if (delta < bestDelta) {
					bestDelta = delta;
					bestPointIndex = pointIndex;
					bestTargetCluster = targetCluster;
				}
			}
		}

		if (bestPointIndex < 0 || bestTargetCluster < 0) break;
		assignments[bestPointIndex] = bestTargetCluster;
		counts[sourceCluster] -= 1;
		counts[bestTargetCluster] += 1;
	}

	return assignments;
}

function initializeKMeansPlusPlus(points: Point2D[], weights: number[], clusterCount: number, random: SeededRandom): Point2D[] {
	if (!points.length) return [];
	const centroids: Point2D[] = [];
	const firstIndex = Math.floor(random() * points.length);
	centroids.push([...points[firstIndex]] as Point2D);

	while (centroids.length < clusterCount) {
		const distances = points.map((point, index) => {
			let best = Number.POSITIVE_INFINITY;
			for (const centroid of centroids) {
				best = Math.min(best, squaredDistance(point, centroid));
			}
			return best * Math.max(0, weights[index] ?? 0);
		});
		const totalDistance = distances.reduce((sum, value) => sum + value, 0);
		if (totalDistance <= 0) {
			const fallbackPoint = points[Math.min(centroids.length, points.length - 1)];
			centroids.push([...fallbackPoint] as Point2D);
			continue;
		}
		let threshold = random() * totalDistance;
		let selectedIndex = distances.length - 1;
		for (let index = 0; index < distances.length; index += 1) {
			threshold -= distances[index];
			if (threshold <= 0) {
				selectedIndex = index;
				break;
			}
		}
		centroids.push([...points[selectedIndex]] as Point2D);
	}

	return centroids;
}

function assignmentsEqual(left: number[], right: number[]): boolean {
	if (left.length !== right.length) return false;
	for (let index = 0; index < left.length; index += 1) {
		if (left[index] !== right[index]) return false;
	}
	return true;
}

function centroidsChanged(left: Point2D[], right: Point2D[], tolerance = 1e-9): boolean {
	if (left.length !== right.length) return true;
	for (let index = 0; index < left.length; index += 1) {
		if (squaredDistance(left[index], right[index]) > tolerance) return true;
	}
	return false;
}

export function browserGeographicKMeans(customers: CustomerRecord[], nClusters: number, iterations = 30): AssignmentRow[] {
	return runBrowserKMeansPreview(customers, nClusters, { maxIterations: iterations }).finalAssignments;
}

function runIterativeKMeansPreview(
	customers: CustomerRecord[],
	nClusters: number,
	options: KMeansOptions = {}
): BrowserKMeansPreview {
	if (!customers.length) {
		return {
			seed: options.seed ?? 0,
			steps: [],
			finalAssignments: [],
			finalCentroids: [],
			iterations: 0,
			converged: true
		};
	}

	const clusterCount = Math.max(1, Math.min(nClusters, customers.length));
	const seed = options.seed ?? Math.floor(Math.random() * 2_147_483_647);
	const random = createSeededRandom(seed);
	const weights = customerWeights(customers, options.demandAware);
	const points = customers.map((customer) => [customer.latitude, customer.longitude] as Point2D);
	const lambda = Math.max(0, options.lambda ?? 0);
	const depotPoint: Point2D | null = options.depot ? [options.depot.latitude, options.depot.longitude] : null;
	const capacities = options.demandAware ? null : balancedClusterCapacities(customers.length, clusterCount);
	let centroids = initializeKMeansPlusPlus(points, weights, clusterCount, random);
	let previousAssignments = new Array<number>(customers.length).fill(-1);
	const steps: ClusterPreviewStep[] = [];

	for (let iteration = 1; iteration <= (options.maxIterations ?? 30); iteration += 1) {
		const scores = assignmentScores(points, centroids, depotPoint, lambda);
		const assignments = capacities
			? rebalanceAssignmentsToCapacities(scores, capacities)
			: nearestClusterAssignments(scores);

		const clusterSums = new Array(clusterCount).fill(null).map(() => ({
			latitude: 0,
			longitude: 0,
			totalWeight: 0
		}));
		assignments.forEach((clusterId, index) => {
			const weight = weights[index] ?? 1;
			const sum = clusterSums[clusterId];
			sum.latitude += points[index][0] * weight;
			sum.longitude += points[index][1] * weight;
			sum.totalWeight += weight;
		});

		const nextCentroids = clusterSums.map((sum, clusterId) => {
			if (sum.totalWeight <= 0) return centroids[clusterId];
			const denominator = sum.totalWeight + (depotPoint ? lambda : 0);
			return [
				(sum.latitude + (depotPoint ? lambda * depotPoint[0] : 0)) / denominator,
				(sum.longitude + (depotPoint ? lambda * depotPoint[1] : 0)) / denominator
			] as Point2D;
		});

		const converged = assignmentsEqual(assignments, previousAssignments) && !centroidsChanged(nextCentroids, centroids);
		const assignmentRows = assignmentsFromClusters(customers, assignments);
		steps.push({
			iteration,
			assignments: assignmentRows,
			centroids: centroidsFromResult(nextCentroids, assignments),
			converged
		});

		centroids = nextCentroids;
		previousAssignments = assignments;
		if (converged) break;
	}

	const finalStep = steps[steps.length - 1] ?? {
		iteration: 0,
		assignments: assignmentsFromClusters(customers, new Array(customers.length).fill(0)),
		centroids: [],
		converged: false
	};

	return {
		seed,
		steps,
		finalAssignments: finalStep.assignments,
		finalCentroids: finalStep.centroids,
		iterations: finalStep.iteration,
		converged: finalStep.converged
	};
}

export function runBrowserKMeansPreview(
	customers: CustomerRecord[],
	nClusters: number,
	options: { maxIterations?: number; seed?: number; demandAware?: boolean } = {}
): BrowserKMeansPreview {
	return runIterativeKMeansPreview(customers, nClusters, options);
}

export function runDepotRegularizedKMeansPreview(
	customers: CustomerRecord[],
	nClusters: number,
	depot: { latitude: number; longitude: number },
	options: { maxIterations?: number; seed?: number; lambda?: number; demandAware?: boolean } = {}
): BrowserKMeansPreview {
	return runIterativeKMeansPreview(customers, nClusters, {
		...options,
		depot,
		lambda: Math.max(0, options.lambda ?? 4)
	});
}

function assignmentsFromHierarchicalGroups(customers: CustomerRecord[], groups: Cluster[]): AssignmentRow[] {
	const assignments = new Array<number>(customers.length).fill(0);
	groups.forEach((group, clusterId) => {
		for (const index of group.indices()) {
			assignments[index] = clusterId;
		}
	});
	return assignmentsFromClusters(customers, assignments);
}

function buildHierarchicalStepCounts(totalCustomers: number, targetClusters: number, maxSteps = 18): number[] {
	const counts: number[] = [];
	const start = totalCustomers;
	const end = Math.max(1, targetClusters);
	if (start <= end) return [end];
	const span = start - end;
	if (span + 1 <= maxSteps) {
		for (let count = start; count >= end; count -= 1) counts.push(count);
		return counts;
	}
	for (let step = 0; step < maxSteps; step += 1) {
		const ratio = step / Math.max(1, maxSteps - 1);
		const count = Math.round(start - ratio * span);
		if (!counts.includes(count)) counts.push(count);
	}
	if (!counts.includes(end)) counts.push(end);
	return counts.sort((a, b) => b - a);
}

export function runBrowserHierarchicalPreview(
	customers: CustomerRecord[],
	nClusters: number,
	options: { method?: AgglomerationMethod; maxAnimationSteps?: number } = {}
): BrowserHierarchicalPreview {
	const linkage = options.method ?? 'ward';
	if (!customers.length) {
		return {
			linkage,
			steps: [],
			finalAssignments: [],
			finalCentroids: [],
			iterations: 0,
			converged: true,
			finalHeight: 0
		};
	}

	const clusterCount = Math.max(1, Math.min(nClusters, customers.length));
	const points = customers.map((customer) => [customer.latitude, customer.longitude]);
	const tree = agnes(points, { method: linkage });
	const stepCounts = buildHierarchicalStepCounts(customers.length, clusterCount, options.maxAnimationSteps ?? 18);
	const steps: ClusterPreviewStep[] = stepCounts.map((groupCount, index) => {
		const groupedRoot = tree.group(groupCount);
		const groups = groupedRoot.children.length ? groupedRoot.children : [groupedRoot];
		const assignments = assignmentsFromHierarchicalGroups(customers, groups);
		return {
			iteration: index + 1,
			assignments,
			centroids: centroidsFromAssignments(customers, assignments),
			converged: groupCount === clusterCount
		};
	});

	const finalStep = steps[steps.length - 1] ?? {
		iteration: 0,
		assignments: assignmentsFromClusters(customers, new Array(customers.length).fill(0)),
		centroids: centroidsFromAssignments(customers, assignmentsFromClusters(customers, new Array(customers.length).fill(0))),
		converged: false
	};

	return {
		linkage,
		steps,
		finalAssignments: finalStep.assignments,
		finalCentroids: finalStep.centroids,
		iterations: finalStep.iteration,
		converged: finalStep.converged,
		finalHeight: tree.height
	};
}

type AngularCustomer = {
	customer: CustomerRecord;
	index: number;
	angle: number;
};

type GapInfo = {
	index: number;
	gapSize: number;
};

function angularGaps(sorted: AngularCustomer[]): GapInfo[] {
	return sorted.map((point, index) => {
		const nextPoint = sorted[(index + 1) % sorted.length];
		let gap = nextPoint.angle - point.angle;
		if (gap < 0) gap += 2 * Math.PI;
		return { index, gapSize: gap };
	});
}

function startIndexFromLargestGap(sorted: AngularCustomer[]): number {
	if (!sorted.length) return 0;
	const largestGap = angularGaps(sorted).slice().sort((a, b) => b.gapSize - a.gapSize)[0];
	return ((largestGap?.index ?? (sorted.length - 1)) + 1) % sorted.length;
}

function assignmentsFromAngularCountCuts(sorted: AngularCustomer[], clusterCount: number): number[] {
	const assignments = new Array<number>(sorted.length).fill(0);
	if (!sorted.length) return assignments;

	const startIndex = startIndexFromLargestGap(sorted);
	const totalItems = sorted.length;
	const targetItems = totalItems / Math.max(1, clusterCount);
	let clusterId = 0;
	let cumulativeItems = 0;
	let nextBoundary = targetItems;

	for (let offset = 0; offset < sorted.length; offset += 1) {
		const sortedIndex = (startIndex + offset) % sorted.length;
		const originalIndex = sorted[sortedIndex].index;
		assignments[originalIndex] = clusterId;
		cumulativeItems += 1;

		const itemsRemaining = totalItems - offset - 1;
		const clustersRemaining = clusterCount - clusterId - 1;
		if (
			clusterId < clusterCount - 1 &&
			cumulativeItems >= nextBoundary - 1e-9 &&
			itemsRemaining >= clustersRemaining
		) {
			clusterId += 1;
			nextBoundary = targetItems * (clusterId + 1);
		}
	}

	return assignments;
}

function demandBalancedAngularAssignmentsForStart(
	sorted: AngularCustomer[],
	clusterCount: number,
	weights: number[],
	startIndex: number
): number[] {
	const assignments = new Array<number>(sorted.length).fill(0);
	if (!sorted.length) return assignments;

	const ordered = sorted.map((_, offset) => sorted[(startIndex + offset) % sorted.length]);
	let cursor = 0;
	let remainingWeight = ordered.reduce((sum, entry) => sum + (weights[entry.index] ?? 1), 0);

	for (let clusterId = 0; clusterId < clusterCount; clusterId += 1) {
		const clustersLeft = clusterCount - clusterId;
		if (clustersLeft <= 1) {
			for (; cursor < ordered.length; cursor += 1) {
				assignments[ordered[cursor].index] = clusterId;
			}
			break;
		}

		const targetWeight = remainingWeight / clustersLeft;
		let sliceWeight = 0;
		let sliceSize = 0;

		while (cursor < ordered.length) {
			const clustersRemaining = clusterCount - clusterId - 1;
			if (sliceSize > 0 && ordered.length - cursor <= clustersRemaining) {
				break;
			}

			const current = ordered[cursor];
			const currentWeight = weights[current.index] ?? 1;
			assignments[current.index] = clusterId;
			sliceWeight += currentWeight;
			sliceSize += 1;
			cursor += 1;

			const itemsRemaining = ordered.length - cursor;
			remainingWeight -= currentWeight;

			if (cursor >= ordered.length) break;

			const next = ordered[cursor];
			const nextWeight = weights[next.index] ?? 1;
			const stopHereDelta = Math.abs(sliceWeight - targetWeight);
			const includeNextDelta = Math.abs(sliceWeight + nextWeight - targetWeight);
			if (sliceSize > 0 && stopHereDelta <= includeNextDelta + 1e-9) {
				break;
			}
		}
	}

	return assignments;
}

function demandBalanceScore(assignments: number[], weights: number[], clusterCount: number): number {
	const clusterWeights = new Array<number>(clusterCount).fill(0);
	for (let index = 0; index < assignments.length; index += 1) {
		const clusterId = assignments[index] ?? 0;
		clusterWeights[clusterId] += weights[index] ?? 1;
	}

	const totalWeight = clusterWeights.reduce((sum, value) => sum + value, 0);
	const targetWeight = totalWeight / Math.max(1, clusterCount);
	return clusterWeights.reduce((sum, value) => {
		const delta = value - targetWeight;
		return sum + delta * delta;
	}, 0);
}

function startGapSize(sorted: AngularCustomer[], startIndex: number): number {
	if (!sorted.length) return 0;
	const gapIndex = (startIndex - 1 + sorted.length) % sorted.length;
	return angularGaps(sorted)[gapIndex]?.gapSize ?? 0;
}

function assignmentsFromAngularDemandCuts(
	sorted: AngularCustomer[],
	clusterCount: number,
	weights: number[]
): number[] {
	if (!sorted.length) return new Array<number>(sorted.length).fill(0);
	if (clusterCount <= 1) return new Array<number>(sorted.length).fill(0);

	const preferredStartIndex = startIndexFromLargestGap(sorted);
	let bestAssignments = demandBalancedAngularAssignmentsForStart(sorted, clusterCount, weights, preferredStartIndex);
	let bestScore = demandBalanceScore(bestAssignments, weights, clusterCount);
	let bestStartIndex = preferredStartIndex;

	for (let startIndex = 0; startIndex < sorted.length; startIndex += 1) {
		if (startIndex === preferredStartIndex) continue;
		const candidateAssignments = demandBalancedAngularAssignmentsForStart(sorted, clusterCount, weights, startIndex);
		const candidateScore = demandBalanceScore(candidateAssignments, weights, clusterCount);
		const scoreImproved = candidateScore + 1e-9 < bestScore;
		const scoreTied = Math.abs(candidateScore - bestScore) <= 1e-9;
		const gapPreferred =
			scoreTied && startGapSize(sorted, startIndex) > startGapSize(sorted, bestStartIndex) + 1e-9;
		if (scoreImproved || gapPreferred) {
			bestAssignments = candidateAssignments;
			bestScore = candidateScore;
			bestStartIndex = startIndex;
		}
	}

	return bestAssignments;
}

export function runAngularGapPreview(
	customers: CustomerRecord[],
	nClusters: number,
	center: { latitude: number; longitude: number },
	options: { demandAware?: boolean } = {}
): BrowserAngularGapPreview {
	if (!customers.length) {
		return {
			steps: [],
			finalAssignments: [],
			finalCentroids: [],
			iterations: 0,
			converged: true,
			selectedGapSizes: []
		};
	}

	const clusterCount = Math.max(1, Math.min(nClusters, customers.length));
	const weights = customerWeights(customers, options.demandAware);
	const sorted = customers
		.map((customer, index) => ({
			customer,
			index,
			angle: Math.atan2(customer.latitude - center.latitude, customer.longitude - center.longitude)
		}))
		.sort((a, b) => a.angle - b.angle);

	const gaps = angularGaps(sorted);
	const largestGap = gaps.slice().sort((a, b) => b.gapSize - a.gapSize)[0] ?? null;

	const steps: ClusterPreviewStep[] = [];
	for (let stepIndex = 0; stepIndex < clusterCount; stepIndex += 1) {
		const groupCount = stepIndex + 1;
		const assignments = assignmentsFromClusters(
			customers,
			options.demandAware
				? assignmentsFromAngularDemandCuts(sorted, groupCount, weights)
				: assignmentsFromAngularCountCuts(sorted, groupCount)
		);
		steps.push({
			iteration: groupCount,
			assignments,
			centroids: centroidsFromAssignments(customers, assignments),
			converged: groupCount === clusterCount
		});
	}

	const finalStep = steps[steps.length - 1] ?? {
		iteration: 1,
		assignments: assignmentsFromClusters(customers, new Array(customers.length).fill(0)),
		centroids: centroidsFromAssignments(customers, assignmentsFromClusters(customers, new Array(customers.length).fill(0))),
		converged: true
	};

	return {
		steps,
		finalAssignments: finalStep.assignments,
		finalCentroids: finalStep.centroids,
		iterations: finalStep.iteration,
		converged: finalStep.converged,
		selectedGapSizes: largestGap ? [largestGap.gapSize] : []
	};
}

export function assignmentsToCsv(rows: AssignmentRow[]): string {
	const header = 'client_num,cluster_id,super_cluster_id';
	const lines = rows
		.slice()
		.sort((a, b) => a.client_num - b.client_num)
		.map((row) => `${row.client_num},${row.cluster_id},${row.super_cluster_id}`);
	return `${header}\n${lines.join('\n')}\n`;
}
