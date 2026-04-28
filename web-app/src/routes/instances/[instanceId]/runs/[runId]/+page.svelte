<script lang="ts">
	import { apiFetch } from '$lib/api';
	import { appState } from '$lib/app-state';
	import CsppParameterEditor, {
		defaultCsppRunParameters,
		normalizeCsppRunParameters,
		serializeCsppRunParameters,
		type CsppRunParameters
	} from '../../../../_components/CsppParameterEditor.svelte';
	import ObjectiveTrajectoryChart from '$lib/components/ObjectiveTrajectoryChart.svelte';
	import ScenarioComparisonChart from '$lib/components/ScenarioComparisonChart.svelte';
	import StageSolutionMap from '$lib/components/StageSolutionMap.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import TrendLineChart from '$lib/components/TrendLineChart.svelte';
	import { formatDuration, formatNumber, titleCase } from '$lib/format';
	import { estimatePipelineRuntimeFallbackSec, estimatePipelineRuntimeForSelectionSec } from '$lib/runtime-estimates';
	import { page } from '$app/state';
	import type { PipelineRuntimeEstimate, RuntimeInfo } from '$lib/types';
	import { onMount } from 'svelte';

	const instanceId = $derived(page.params.instanceId ?? null);
	const runId = $derived(page.params.runId ?? null);
	const pipelineProgress = $derived($appState.pipelineProgress);
	const instancePayload = $derived(($appState.instancePayload as Record<string, unknown> | null) ?? null);
	const runConfig = $derived(($appState.runConfig as Record<string, unknown> | null) ?? null);
	let currentTimeMs = $state(Date.now());
	let runtimes = $state<RuntimeInfo[]>([]);
	let selectedRuntimeId = $state('local');
	let selectedStage2ScenarioId = $state('');
	let runParameters = $state<CsppRunParameters>(defaultCsppRunParameters());
	let parametersInitializedForRun = $state<string | null>(null);

	onMount(() => {
		appState.selectRun(runId);
		appState.ensureMapData();
		void loadRuntimes();
		const timer = setInterval(() => {
			currentTimeMs = Date.now();
		}, 1000);
		return () => clearInterval(timer);
	});

	const pipelineJob = $derived((pipelineProgress?.job as Record<string, unknown> | undefined) ?? {});
	const pipelineEstimate = $derived((pipelineProgress?.estimate as PipelineRuntimeEstimate | undefined) ?? {});
	const stage1 = $derived($appState.stage1 as Record<string, unknown> | null);
	const stage2 = $derived($appState.stage2 as Record<string, unknown> | null);
	const stage3 = $derived($appState.stage3 as Record<string, unknown> | null);
	const overview = $derived(($appState.overview as Record<string, unknown> | null) ?? null);
	const mapData = $derived($appState.mapData);
	const mapSummary = $derived($appState.mapSummary);
	const stageStatus = $derived((overview?.stage_status as Record<string, unknown> | undefined) ?? {});

	let isTriggeringPipeline = $state(false);
	let isStoppingPipeline = $state(false);
	let pipelineActionStatus = $state('');

	async function loadRuntimes(): Promise<void> {
		try {
			const response = await apiFetch('/api/runtimes');
			if (!response.ok) return;
			const payload = (await response.json()) as { runtimes?: RuntimeInfo[] };
			runtimes = payload.runtimes ?? [];
			const activeRuntimeId = String(pipelineJob.runtime_id ?? '');
			if (activeRuntimeId && runtimes.some((runtime) => runtime.id === activeRuntimeId)) {
				selectedRuntimeId = activeRuntimeId;
				return;
			}
			if (!runtimes.some((runtime) => runtime.id === selectedRuntimeId)) {
				selectedRuntimeId = runtimes[0]?.id ?? 'local';
			}
			await probeSelectedRuntime();
		} catch {
			runtimes = runtimes;
		}
	}

	async function probeSelectedRuntime(): Promise<void> {
		if (!selectedRuntimeId) return;
		try {
			const response = await apiFetch(`/api/runtimes/${encodeURIComponent(selectedRuntimeId)}/probe`, {
				method: 'POST'
			});
			if (!response.ok) return;
			const payload = (await response.json()) as { probe?: RuntimeInfo['probe']; queue?: RuntimeInfo['queue'] };
			runtimes = runtimes.map((runtime) =>
				runtime.id === selectedRuntimeId
					? {
							...runtime,
							probe: payload.probe ?? runtime.probe,
							queue: payload.queue ?? runtime.queue
						}
					: runtime
			);
		} catch {
			return;
		}
	}

	async function triggerPipeline(): Promise<void> {
		const targetInstanceId = String(runConfig?.instance_id ?? instanceId ?? $appState.selectedInstanceId ?? '').trim();
		if (!targetInstanceId) return;
		isTriggeringPipeline = true;
		pipelineActionStatus = '';
		try {
			const formData = new FormData();
			formData.set('runtime_id', selectedRuntimeId || 'local');
			formData.set('parameters_json', JSON.stringify(serializeCsppRunParameters(runParameters)));
			const response = await apiFetch(`/api/instances/${encodeURIComponent(targetInstanceId)}/runs`, {
				method: 'POST',
				body: formData
			});
			const text = await response.text();
			let payload: Record<string, unknown> | null = null;
			try {
				payload = JSON.parse(text) as Record<string, unknown>;
			} catch {
				if (!response.ok) {
					throw new Error(text || `Trigger failed: ${response.status}`);
				}
				throw new Error('Trigger returned invalid JSON.');
			}
			if (!response.ok) {
				const detail = payload ? payload.detail : undefined;
				throw new Error(typeof detail === 'string' ? detail : `Trigger failed: ${response.status}`);
			}
			const nextRunId = typeof payload?.run_id === 'string' ? payload.run_id : runId;
			if (nextRunId && nextRunId !== runId) {
				appState.selectRun(nextRunId);
				window.location.href = `/instances/${targetInstanceId}/runs/${nextRunId}`;
				return;
			}
			appState.refreshNow();
			appState.requestSync();
		} catch (error) {
			console.error('Failed to trigger pipeline', error);
			pipelineActionStatus = error instanceof Error ? error.message : String(error);
		} finally {
			isTriggeringPipeline = false;
			appState.requestSync();
		}
	}

	async function stopPipeline(): Promise<void> {
		if (!runId) return;
		isStoppingPipeline = true;
		try {
			const response = await apiFetch(`/api/runs/${encodeURIComponent(runId)}/stop`, {
				method: 'POST'
			});
			const text = await response.text();
			if (!response.ok) {
				let detail = `Stop failed: ${response.status}`;
				try {
					const payload = JSON.parse(text) as { detail?: string };
					if (typeof payload.detail === 'string' && payload.detail.trim()) detail = payload.detail;
				} catch {
					if (text.trim()) detail = text.trim();
				}
				throw new Error(detail);
			}
			appState.refreshNow();
			appState.requestSync();
			await loadRuntimes();
		} catch (error) {
			console.error('Failed to stop pipeline', error);
		} finally {
			isStoppingPipeline = false;
			appState.requestSync();
		}
	}
	const pipelineLogTail = $derived((pipelineProgress?.log_tail as string[] | undefined) ?? []);
	const pipelineStatusLabel = $derived(String(pipelineJob.status ?? 'idle'));
	const pipelineElapsedSeconds = $derived.by(() => {
		const startedAtValue = pipelineJob.started_at;
		if (!startedAtValue) return null;
		const startedAtMs = new Date(String(startedAtValue)).getTime();
		if (Number.isNaN(startedAtMs)) return null;

		const finishedAtValue = pipelineJob.finished_at;
		const finishedAtMs = finishedAtValue ? new Date(String(finishedAtValue)).getTime() : Number.NaN;
		const endMs = Number.isNaN(finishedAtMs) ? currentTimeMs : finishedAtMs;
		return Math.max(0, Math.floor((endMs - startedAtMs) / 1000));
	});
	const pipelineTotalEstimateSec = $derived(pipelineEstimate?.estimated_total_sec ?? null);
	const pipelineRemainingEstimateSec = $derived(pipelineEstimate?.estimated_remaining_sec ?? null);
	const pipelineEstimateSummary = $derived.by(() => {
		const totalRuns = pipelineEstimate?.total_runs;
		const usableCores = pipelineEstimate?.usable_cores;
		if (pipelineTotalEstimateSec == null && totalRuns == null && usableCores == null) return null;
		const parts = [];
		if (pipelineTotalEstimateSec != null) parts.push(`Est. ${formatDuration(pipelineTotalEstimateSec)}`);
		if (totalRuns != null) parts.push(`${formatNumber(totalRuns, 0)} runs`);
		if (usableCores != null) parts.push(`${formatNumber(usableCores, 0)} parallel slots`);
		return parts.join(' · ');
	});
	const activeRuntime = $derived.by(() => runtimes.find((runtime) => runtime.id === selectedRuntimeId) ?? null);
	const selectedRuntimeUsableCores = $derived.by(() => {
		const fromProbe = activeRuntime?.probe?.usable_cores;
		return typeof fromProbe === 'number' && Number.isFinite(fromProbe) ? fromProbe : null;
	});
	const selectedRuntimeEstimateSlots = $derived.by(() => {
		const cores = selectedRuntimeUsableCores ?? 1;
		return runParameters.compute_profile === 'light' ? Math.max(1, Math.floor(cores / 2)) : cores;
	});
	const fallbackEstimateTotalSec = $derived.by(() => {
		return estimatePipelineRuntimeFallbackSec(instancePayload, runParameters as unknown as Record<string, unknown>, selectedRuntimeEstimateSlots);
	});
	const estimatedRuntimeForSelectionSec = $derived.by(() => {
		return estimatePipelineRuntimeForSelectionSec(pipelineEstimate, fallbackEstimateTotalSec, selectedRuntimeEstimateSlots);
	});
	const selectedRuntimeEstimateSummary = $derived.by(() => {
		const totalRuns = pipelineEstimate?.total_runs;
		const selectedCores = selectedRuntimeEstimateSlots;
		const parts = [];
		if (estimatedRuntimeForSelectionSec != null) parts.push(`Est. ${formatDuration(estimatedRuntimeForSelectionSec)}`);
		if (totalRuns != null) parts.push(`${formatNumber(totalRuns, 0)} runs`);
		if (selectedCores != null) {
			parts.push(`${formatNumber(selectedCores, 0)} cores`);
		} else if (estimatedRuntimeForSelectionSec != null) {
			parts.push('assuming 1 slot');
		}
		return parts.length ? parts.join(' · ') : null;
	});
	const canStopPipeline = $derived.by(() =>
		['queued', 'preparing', 'syncing_to_runtime', 'running', 'syncing_from_runtime'].includes(pipelineStatusLabel)
	);

	$effect(() => {
		if (!selectedRuntimeId) return;
		void probeSelectedRuntime();
	});

	$effect(() => {
		if (!runId || parametersInitializedForRun === runId || !runConfig) return;
		parametersInitializedForRun = runId;
		runParameters = normalizeCsppRunParameters(runConfig);
	});

	type ClusterRow = Record<string, unknown>;
	type ClusterDetail = {
		cluster?: ClusterRow;
		live_state?: Record<string, unknown>;
		events?: Array<Record<string, unknown>>;
	};

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
			if (objective === null) objective = asNumber(event.master_obj_val) ?? asNumber(event.first_stage_objective);
			if (runtimeSec === null) runtimeSec = asNumber(event.master_runtime_sec);
			if (runtimeSec === null || objective === null || objective <= 0 || objective >= 1e50) continue;
			const points = pointsByIteration.get(iteration) ?? [];
			points.push({ x: runtimeSec, y: objective });
			pointsByIteration.set(iteration, points);
		}
		const sortedIterations = [...pointsByIteration.keys()].sort((a, b) => a - b);
		const fallbackCurrentIteration = currentIteration ?? (sortedIterations.length ? sortedIterations[sortedIterations.length - 1] : null);
		return sortedIterations.map((iteration) => {
			const sorted = (pointsByIteration.get(iteration) ?? [])
				.sort((left, right) => left.x - right.x)
				.filter((point, index, points) => index === 0 || point.x !== points[index - 1].x || point.y !== points[index - 1].y);
			const xOffset = sorted.length ? sorted[0].x : 0;
			return {
				iteration,
				isCurrent: fallbackCurrentIteration === iteration,
				points: sorted.map((point) => ({ x: point.x - xOffset, y: point.y }))
			};
		});
	}

	const stage1Clusters = $derived(((stage1?.clusters as ClusterRow[]) ?? []));
	const stage1CombinedSolution = $derived((stage1?.combined_solution as Record<string, unknown> | undefined) ?? null);
	const stage1Details = $derived($appState.details.stage1Clusters as Record<string, ClusterDetail>);
	$effect(() => {
		for (const cluster of stage1Clusters.slice(0, 4)) {
			const clusterId = String(cluster.cluster_id ?? '');
			if (clusterId) appState.ensureStage1Cluster(clusterId);
		}
	});
	const overviewStage1Charts = $derived.by(() =>
		stage1Clusters
			.map((cluster) => {
				const clusterId = String(cluster.cluster_id ?? 'n/a');
				const detail = stage1Details[clusterId];
				return {
					clusterId,
					subtitle: `${titleCase(String(cluster.status ?? 'unknown'))} · Iteration ${String(cluster.current_iteration ?? cluster.iterations ?? 'n/a')}`,
					series: buildObjectiveSeries(detail, cluster),
					sortScore: Number(cluster.is_active ? 1000 : 0) + Number(cluster.current_iteration ?? cluster.iterations ?? 0)
				};
			})
			.sort((left, right) => right.sortScore - left.sortScore)
			.slice(0, 4)
	);

	const stage2Scenarios = $derived((stage2?.scenarios as Record<string, unknown>[] | undefined) ?? []);
	const stage3Dashboard = $derived((stage3?.dashboard as Record<string, unknown> | undefined) ?? {});
	const showStage3Section = $derived.by(() => {
		const status = String(stageStatus.reoptimization ?? '').trim().toLowerCase();
		if (status === 'missing' || status === 'skipped') return false;
		return Boolean(stage3Dashboard && Object.keys(stage3Dashboard).length);
	});

	function scenarioCost(row: Record<string, unknown>): number {
		const parsed = Number(row.total_cost);
		return Number.isFinite(parsed) ? parsed : -1;
	}

	const stage2DefaultScenarioId = $derived.by(() => {
		const viable = [...stage2Scenarios].filter((row) => row.total_cost !== null && row.total_cost !== undefined);
		const selected = viable.sort((left, right) => scenarioCost(right) - scenarioCost(left))[0] ?? stage2Scenarios[0];
		return selected ? String(selected.scenario_id ?? '') : '';
	});
	$effect(() => {
		if (!selectedStage2ScenarioId && stage2DefaultScenarioId) selectedStage2ScenarioId = stage2DefaultScenarioId;
	});
	$effect(() => {
		if (selectedStage2ScenarioId) appState.ensureStage2Scenario(selectedStage2ScenarioId);
	});
	const stage2SelectedScenario = $derived(stage2Scenarios.find((row) => String(row.scenario_id ?? '') === selectedStage2ScenarioId) ?? null);
	const stage2RouteBundle = $derived(
		selectedStage2ScenarioId ? ($appState.details.stage2Scenarios[selectedStage2ScenarioId]?.route_bundle as Record<string, unknown> | undefined) ?? null : null
	);
	const stage3TrendSeries = $derived((stage3Dashboard?.trend_series as Record<string, unknown>[] | undefined) ?? []);
	const stage3ScenarioComparison = $derived((stage3Dashboard?.scenario_comparison as Record<string, unknown>[] | undefined) ?? []);
	const stage3ChargerDeltaBundle = $derived((stage3Dashboard?.charger_delta_bundle as Record<string, unknown> | undefined) ?? null);
</script>

<section class="panel stack">
	<div class="section-title">
		<div>
			<h1>{runId}</h1>
		</div>
		<div class="header-actions">
			<label class="runtime-select">
				<span>Runtime</span>
				<select bind:value={selectedRuntimeId}>
					{#each runtimes as runtime}
						<option value={runtime.id}>{runtime.label ?? runtime.id}</option>
					{/each}
				</select>
			</label>
			<button type="button" class="primary-action" onclick={() => void triggerPipeline()} disabled={isTriggeringPipeline || pipelineStatusLabel === 'running' || pipelineStatusLabel === 'queued'}>
				{#if isTriggeringPipeline}
					Starting...
				{:else if pipelineStatusLabel === 'running' || pipelineStatusLabel === 'queued'}
					Solve Running
				{:else if (activeRuntime?.queue?.active_run_id ?? null) && pipelineJob.runtime_id !== selectedRuntimeId}
					Queue New Run
				{:else}
					Start New Run
				{/if}
			</button>
			{#if canStopPipeline}
				<button type="button" class="secondary-action" onclick={() => void stopPipeline()} disabled={isStoppingPipeline}>
					{#if isStoppingPipeline}
						Stopping...
					{:else}
						Stop Solve
					{/if}
				</button>
			{/if}
			{#if selectedRuntimeEstimateSummary ?? pipelineEstimateSummary}
				<p class="estimate-chip">{selectedRuntimeEstimateSummary ?? pipelineEstimateSummary}</p>
			{/if}
			{#if pipelineActionStatus}
				<p class="action-status">{pipelineActionStatus}</p>
			{/if}
		</div>
	</div>
</section>

<section class="panel stack">
	<div class="section-title">
		<div>
			<h2>Configuration</h2>
			<p class="muted">New runs started here use these values in run_config.json.</p>
		</div>
	</div>
	<CsppParameterEditor value={runParameters} onchange={(value) => (runParameters = value)} disabled={isTriggeringPipeline} detectedCores={selectedRuntimeUsableCores} />
</section>

<section class="panel stack">
	<div class="section-title">
		<div>
			<h2>Solve Pipeline</h2>
		</div>
		<StatusPill value={pipelineStatusLabel} />
	</div>

	<div class="pipeline-meta">
		<div><span>Stage</span><strong>{String(pipelineJob.current_stage_label ?? pipelineJob.current_stage_key ?? 'idle')}</strong></div>
		<div><span>Elapsed</span><strong>{formatDuration(pipelineElapsedSeconds)}</strong></div>
		<div><span>Estimate</span><strong>{formatDuration(pipelineTotalEstimateSec)}</strong></div>
		<div><span>Remaining</span><strong>{formatDuration(pipelineRemainingEstimateSec)}</strong></div>
		<div><span>Runtime</span><strong>{String(pipelineJob.runtime_label ?? activeRuntime?.label ?? selectedRuntimeId ?? 'local')}</strong></div>
		<div><span>Cores</span><strong>{formatNumber((pipelineJob.runtime_usable_cores as number | null | undefined) ?? activeRuntime?.probe?.usable_cores ?? null, 0)}</strong></div>
	</div>

	{#if pipelineJob.error}
		<div class="pipeline-error">{String(pipelineJob.error)}</div>
	{/if}

	{#if pipelineLogTail.length}
		<details class="log-details">
			<summary>Pipeline log ({pipelineLogTail.length} lines)</summary>
			<pre class="pipeline-log">{pipelineLogTail.join('\n')}</pre>
		</details>
	{/if}
</section>

<section class="panel stack">
	<div class="section-title">
		<h2>Stage 1: Cluster Solve</h2>
		<a href={`/instances/${instanceId}/runs/${runId}/stage-1`}>Open stage</a>
	</div>
	{#if overviewStage1Charts.length}
		<div class="overview-chart-grid">
			{#each overviewStage1Charts as chart}
				<ObjectiveTrajectoryChart
					title={`Cluster ${chart.clusterId}`}
					subtitle={chart.subtitle}
					series={chart.series}
					xLabel="Runtime"
					yLabel="Objective"
					yScaleType="log"
				/>
			{/each}
		</div>
	{/if}
	<StageSolutionMap
		title="Combined Installed Chargers"
		subtitle="Aggregated first-stage charger decisions across all clusters"
		data={mapData}
		summary={mapSummary}
		bundle={stage1CombinedSolution}
		height={420}
	/>
</section>

<section class="panel stack">
	<div class="section-title">
		<h2>Stage 2: Scenario Evaluation</h2>
		<a href={`/instances/${instanceId}/runs/${runId}/stage-2`}>Open stage</a>
	</div>
	{#if stage2Scenarios.length}
		<div class="selector-row">
			<label>
				<span>Scenario</span>
				<select bind:value={selectedStage2ScenarioId}>
					{#each stage2Scenarios as scenario}
						<option value={String(scenario.scenario_id ?? '')}>Scenario {String(scenario.scenario_id ?? 'n/a')}</option>
					{/each}
				</select>
			</label>
		</div>
	{/if}
	<StageSolutionMap
		title={`Scenario ${selectedStage2ScenarioId || 'n/a'} Routes`}
		subtitle={stage2SelectedScenario?.total_cost != null
			? `Status ${titleCase(String(stage2SelectedScenario?.status ?? 'unknown'))} · Cost ${formatNumber(Number(stage2SelectedScenario.total_cost), 0)}`
			: `Status ${titleCase(String(stage2SelectedScenario?.status ?? 'unknown'))}`}
		data={mapData}
		summary={mapSummary}
		bundle={stage2RouteBundle}
		height={420}
	/>
</section>

{#if showStage3Section}
<section class="panel stack">
	<div class="section-title">
		<h2>Stage 3: Cluster Reoptimization</h2>
		<a href={`/instances/${instanceId}/runs/${runId}/stage-3`}>Open stage</a>
	</div>
	<div class="viz-grid">
		<TrendLineChart title="Worst Cost Over Iterations" points={stage3TrendSeries} />
		<ScenarioComparisonChart title="Baseline vs Best Iteration Scenarios" rows={stage3ScenarioComparison} />
	</div>
	<StageSolutionMap
		title="Charger Changes vs Baseline"
		subtitle="Added, removed, and unchanged store chargers between baseline and best iteration"
		data={mapData}
		summary={mapSummary}
		bundle={stage3ChargerDeltaBundle}
		height={420}
	/>
</section>
{/if}

<style>
	.header-actions {
		display: flex;
		align-items: center;
		gap: 0.75rem;
		flex-wrap: wrap;
		justify-content: flex-end;
	}

	.estimate-chip {
		margin: 0;
		padding: 0.65rem 0.8rem;
		border: 1px solid var(--border);
		background: #fafafa;
		font-size: 0.9rem;
		font-variant-numeric: tabular-nums;
	}

	.action-status {
		flex-basis: 100%;
		margin: 0;
		font-size: 0.85rem;
		color: var(--error);
	}

	.secondary-action {
		margin: 0;
		padding: 0.65rem 0.9rem;
		border: 1px solid rgba(239, 68, 68, 0.35);
		border-radius: 0;
		background: rgba(239, 68, 68, 0.08);
		color: #7f1d1d;
		font: inherit;
		cursor: pointer;
	}

	.secondary-action:disabled {
		opacity: 0.65;
		cursor: wait;
	}

	.pipeline-meta {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(8rem, 1fr));
		gap: 0.75rem;
	}

	.runtime-select {
		display: grid;
		gap: 0.3rem;
		font-size: 0.8rem;
		color: var(--muted);
	}

	.runtime-select select {
		min-width: 11rem;
	}

	.pipeline-meta div {
		border: 1px solid var(--border);
		padding: 0.75rem;
		display: grid;
		gap: 0.35rem;
		align-content: start;
	}

	.pipeline-meta strong {
		font-variant-numeric: tabular-nums;
	}

	.pipeline-meta span {
		font-size: 0.8rem;
		color: var(--muted);
		text-transform: uppercase;
		letter-spacing: 0.02em;
	}

	.pipeline-error {
		border: 1px solid var(--error);
		color: var(--error);
		padding: 0.75rem;
	}

	.log-details summary {
		cursor: pointer;
		font-size: 0.85rem;
		color: var(--muted);
		font-weight: 600;
	}

	.pipeline-log {
		margin: 0;
		padding: 0.85rem;
		border: 1px solid var(--border);
		background: #fafafa;
		font-size: 0.82rem;
		line-height: 1.45;
		overflow: auto;
		max-height: 20rem;
		white-space: pre-wrap;
		word-break: break-word;
	}

	.overview-chart-grid {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(min(420px, 100%), 1fr));
		gap: 1rem;
	}

	.viz-grid {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 1rem;
	}

	.selector-row {
		display: flex;
		justify-content: flex-end;
	}

	.selector-row label {
		display: grid;
		gap: 0.3rem;
		font-size: 0.82rem;
		color: var(--muted);
	}

	@media (max-width: 760px) {
		.header-actions,
		.pipeline-meta {
			grid-template-columns: 1fr;
			display: grid;
		}

		.overview-chart-grid,
		.viz-grid {
			grid-template-columns: 1fr;
		}
	}
</style>
