<script lang="ts">
	import { appState } from '$lib/app-state';
	import ClusterRuntimeChart from '$lib/components/ClusterRuntimeChart.svelte';
	import ObjectiveTrajectoryChart from '$lib/components/ObjectiveTrajectoryChart.svelte';
	import MetricGrid from '$lib/components/MetricGrid.svelte';
	import StageSolutionMap from '$lib/components/StageSolutionMap.svelte';
	import { titleCase } from '$lib/format';
	import { page } from '$app/state';
	import { onMount, tick } from 'svelte';
	import { getMirroredText } from '$lib/file-mirror-db';

	type ClusterRow = Record<string, unknown>;
	type ClusterDetail = {
		cluster?: ClusterRow;
		live_state?: Record<string, unknown>;
		events?: Array<Record<string, unknown>>;
	};

	const runId = $derived(page.params.runId ?? null);
	const stage1 = $derived($appState.stage1 as Record<string, unknown> | null);
	const summary = $derived((stage1?.summary as Record<string, unknown> | undefined) ?? {});
	const clusters = $derived(((stage1?.clusters as ClusterRow[]) ?? []));
	const combinedSolution = $derived((stage1?.combined_solution as Record<string, unknown> | undefined) ?? null);
	const mapData = $derived($appState.mapData);
	const mapSummary = $derived($appState.mapSummary);
	const stage1Details = $derived($appState.details.stage1Clusters as Record<string, ClusterDetail>);
	let requestedClusterIds = $state<Record<string, boolean>>({});
	let chartRankByClusterId = $state<Record<string, number>>({});
	let chartRankCounter = $state(0);
	let selectedLogClusterId = $state<string | null>(null);
	let logContent = $state<string | null>(null);
	let logPre = $state<HTMLPreElement | null>(null);
	let logRefreshTimer = $state<ReturnType<typeof setInterval> | null>(null);

	async function loadClusterLog(clusterId: string) {
		const path = `exports/runs/${runId}/05_solve_clusters_first_stage/logs/cluster_${clusterId}.log`;
		const atBottom = logPre ? logPre.scrollHeight - logPre.scrollTop - logPre.clientHeight < 40 : true;
		logContent = await getMirroredText(path);
		await tick();
		if (logPre && atBottom) logPre.scrollTop = logPre.scrollHeight;
	}

	function toggleLog(clusterId: string) {
		if (selectedLogClusterId === clusterId) {
			selectedLogClusterId = null;
			logContent = null;
			if (logRefreshTimer) { clearInterval(logRefreshTimer); logRefreshTimer = null; }
		} else {
			if (logRefreshTimer) clearInterval(logRefreshTimer);
			selectedLogClusterId = clusterId;
			loadClusterLog(clusterId);
			logRefreshTimer = setInterval(() => {
				if (selectedLogClusterId) loadClusterLog(selectedLogClusterId);
			}, 2000);
		}
	}

	onMount(() => {
		appState.selectRun(runId);
		appState.ensureMapData();
	});

	const metrics = $derived([
		{ label: 'Completed', value: summary.completed_clusters },
		{ label: 'Total', value: summary.total_clusters },
		{ label: 'Solved', value: summary.solved_clusters },
		{ label: 'Timeouts', value: summary.timeout_clusters }
	]);

	function asNumber(value: unknown): number | null {
		const num = Number(value);
		return Number.isFinite(num) ? num : null;
	}

	function buildObjectiveSeries(detail: ClusterDetail | undefined, cluster: ClusterRow) {
		const liveState = detail?.live_state ?? {};
		const currentIteration = asNumber(liveState.current_iteration) ?? asNumber(cluster.current_iteration);
		const pointsByIteration = new Map<number, Array<{ x: number; y: number }>>();

		let trackedIteration: number | null = null;
		for (const event of detail?.events ?? []) {
			const explicitIteration = asNumber(event.iteration);
			if (explicitIteration !== null) trackedIteration = explicitIteration;

			const iteration = explicitIteration ?? trackedIteration;
			if (iteration === null) continue;

			let runtimeSec = asNumber(event.runtime_sec);
			let objective = asNumber(event.best_obj);
			if (objective === null) {
				objective = asNumber(event.master_obj_val) ?? asNumber(event.first_stage_objective);
			}
			if (runtimeSec === null) {
				runtimeSec = asNumber(event.master_runtime_sec);
			}
			if (runtimeSec === null || objective === null) continue;
			if (objective >= 1e+50) continue;

			const points = pointsByIteration.get(iteration) ?? [];
			points.push({ x: runtimeSec, y: objective });
			pointsByIteration.set(iteration, points);
		}

		const sortedIterations = [...pointsByIteration.keys()].sort((a, b) => a - b);
		const fallbackCurrentIteration =
			currentIteration ?? (sortedIterations.length ? sortedIterations[sortedIterations.length - 1] : null);

		return sortedIterations.map((iteration) => {
			const sorted = (pointsByIteration.get(iteration) ?? [])
				.sort((left, right) => left.x - right.x)
				.filter((point, index, points) => index === 0 || point.x !== points[index - 1].x || point.y !== points[index - 1].y);
			const xOffset = sorted.length ? sorted[0].x : 0;
			return {
				iteration,
				isCurrent: fallbackCurrentIteration === iteration,
				points: sorted.map((p) => ({ x: p.x - xOffset, y: p.y }))
			};
		});
	}

	const liveCharts = $derived.by(() =>
		clusters.map((cluster) => {
			const clusterId = String(cluster.cluster_id ?? 'n/a');
			const detail = stage1Details[clusterId];
			const liveState = detail?.live_state ?? {};
			const series = buildObjectiveSeries(detail, cluster);
			const currentIteration = asNumber(liveState.current_iteration) ?? asNumber(cluster.current_iteration);
			return {
				clusterId,
				cluster,
				series,
				sortRank: chartRankByClusterId[clusterId] ?? 0,
				subtitle: `${titleCase(String(cluster.status ?? 'unknown'))} · Iteration ${currentIteration ?? 'n/a'}`
			};
		})
	);

	$effect(() => {
		for (const cluster of clusters) {
			const clusterId = String(cluster.cluster_id ?? '');
			if (!clusterId || requestedClusterIds[clusterId]) continue;
			requestedClusterIds[clusterId] = true;
			appState.ensureStage1Cluster(clusterId);
		}
	});

	$effect(() => {
		for (const chart of liveCharts) {
			if (!chartRankByClusterId[chart.clusterId]) {
				chartRankByClusterId[chart.clusterId] = ++chartRankCounter;
			}
		}
	});

	const orderedLiveCharts = $derived.by(() =>
		[...liveCharts].sort((left, right) => {
			if (right.sortRank !== left.sortRank) return right.sortRank - left.sortRank;
			return Number(left.cluster.cluster_id ?? 0) - Number(right.cluster.cluster_id ?? 0);
		})
	);
</script>

<section class="panel stack">
	<div class="section-title"><h1>Stage 1: Cluster Solve</h1></div>
	<MetricGrid items={metrics} />

	<div class="viz-grid">
		<ClusterRuntimeChart title="Cluster Size vs Runtime" clusters={clusters} />
	</div>

	<section class="live-figures stack">
		<div class="section-title">
			<h2>Live Objective Trajectories</h2>
		</div>
		<div class="live-chart-grid">
			{#each orderedLiveCharts as chart}
				<ObjectiveTrajectoryChart
					title={`Cluster ${chart.clusterId}`}
					subtitle={chart.subtitle}
					series={chart.series}
					xLabel="Runtime"
					yLabel="Objective"
					yScaleType="log"
					onclick={() => toggleLog(chart.clusterId)}
				/>
			{/each}
		</div>

		{#if selectedLogClusterId !== null}
			<section class="log-panel">
				<div class="log-header">
					<h3>Log — Cluster {selectedLogClusterId}</h3>
					<button class="log-close" onclick={() => toggleLog(selectedLogClusterId!)}>✕</button>
				</div>
				<pre class="log-content" bind:this={logPre}>{logContent ?? 'Loading…'}</pre>
			</section>
		{/if}
	</section>

	<StageSolutionMap
		title="Combined Installed Chargers"
		subtitle="Aggregated first-stage charger decisions across all clusters"
		data={mapData}
		summary={mapSummary}
		bundle={combinedSolution}
		height={420}
	/>
</section>

<style>
	.viz-grid {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
		gap: 1rem;
	}

	.live-figures {
		margin-top: 0.5rem;
	}

	.live-chart-grid {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 1rem;
	}

	@media (max-width: 960px) {
		.live-chart-grid {
			grid-template-columns: 1fr;
		}
	}

	.log-panel {
		border: 1px solid var(--border);
		background: #1a1e26;
		margin-top: 0.75rem;
		display: flex;
		flex-direction: column;
	}

	.log-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		padding: 0.5rem 0.75rem;
		background: #242830;
		border-bottom: 1px solid #363b44;
	}

	.log-header h3 {
		margin: 0;
		font-size: 0.88rem;
		color: #cdd6e0;
	}

	.log-close {
		background: none;
		border: none;
		color: #8892a0;
		cursor: pointer;
		font-size: 1rem;
		padding: 0.15rem 0.4rem;
	}

	.log-close:hover {
		color: #fff;
	}

	.log-content {
		margin: 0;
		padding: 0.6rem 0.75rem;
		font-family: 'SF Mono', 'Menlo', 'Consolas', monospace;
		font-size: 0.78rem;
		line-height: 1.45;
		color: #cdd6e0;
		overflow-y: auto;
		max-height: 400px;
		white-space: pre;
	}
</style>
