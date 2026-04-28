<script lang="ts">
	import { appState } from '$lib/app-state';
	import MetricGrid from '$lib/components/MetricGrid.svelte';
	import StageSolutionMap from '$lib/components/StageSolutionMap.svelte';
	import { page } from '$app/state';
	import { onMount } from 'svelte';

	const runId = $derived(page.params.runId ?? null);
	const scenarioId = $derived(page.params.scenarioId ?? '');

	onMount(() => {
		appState.selectRun(runId);
		appState.ensureMapData();
		appState.ensureStage2Scenario(scenarioId);
	});

	const detail = $derived($appState.details.stage2Scenarios[scenarioId] ?? null);
	const scenario = $derived((detail?.scenario as Record<string, unknown> | undefined) ?? {});
	const routeBundle = $derived((detail?.route_bundle as Record<string, unknown> | undefined) ?? null);
	const mapData = $derived($appState.mapData);
	const mapSummary = $derived($appState.mapSummary);
	const metrics = $derived([
		{ label: 'Total Cost', value: scenario.total_cost },
		{ label: 'Status', value: scenario.status },
		{ label: 'Cluster Solves', value: scenario.cluster_solves },
		{ label: 'Live Solvers', value: scenario.live_solver_count }
	]);
</script>

<section class="panel stack">
	<div class="section-title"><h1>Stage 2 Scenario {scenarioId}</h1></div>
	<MetricGrid items={metrics} />
	<StageSolutionMap
		title={`Scenario ${scenarioId} Routes`}
		subtitle={`Status ${String(scenario.status ?? 'unknown')} · Cost ${String(scenario.total_cost ?? 'n/a')}`}
		data={mapData}
		summary={mapSummary}
		bundle={routeBundle}
		height={420}
	/>
</section>
