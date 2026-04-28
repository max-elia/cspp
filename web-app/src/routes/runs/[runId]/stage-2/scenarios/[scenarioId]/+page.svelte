<script lang="ts">
	import { appState } from '$lib/app-state';
	import MetricGrid from '$lib/components/MetricGrid.svelte';
	import { page } from '$app/state';
	import { onMount } from 'svelte';

	const runId = $derived(page.params.runId ?? null);
	const scenarioId = $derived(page.params.scenarioId ?? '');

	onMount(() => {
		appState.selectRun(runId);
		appState.ensureStage2Scenario(scenarioId);
	});

	const detail = $derived($appState.details.stage2Scenarios[scenarioId] ?? null);
	const scenario = $derived((detail?.scenario as Record<string, unknown> | undefined) ?? {});
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
</section>
