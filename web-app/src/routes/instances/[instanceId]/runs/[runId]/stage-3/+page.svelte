<script lang="ts">
	import { appState } from '$lib/app-state';
	import IterationCostEvolutionChart from '$lib/components/IterationCostEvolutionChart.svelte';
	import MetricGrid from '$lib/components/MetricGrid.svelte';
	import ScenarioComparisonChart from '$lib/components/ScenarioComparisonChart.svelte';
	import StageSolutionMap from '$lib/components/StageSolutionMap.svelte';
	import TrendLineChart from '$lib/components/TrendLineChart.svelte';
	import { page } from '$app/state';
	import { onMount } from 'svelte';

	const runId = $derived(page.params.runId ?? null);
	const stage3 = $derived($appState.stage3 as Record<string, unknown> | null);
	const currentState = $derived((stage3?.current_state as Record<string, unknown> | undefined) ?? {});
	const scopeIds = $derived((stage3?.active_scope_ids as string[] | undefined) ?? []);
	const dashboard = $derived((stage3?.dashboard as Record<string, unknown> | undefined) ?? {});
	const trendSeries = $derived((dashboard?.trend_series as Record<string, unknown>[] | undefined) ?? []);
	const scenarioComparison = $derived((dashboard?.scenario_comparison as Record<string, unknown>[] | undefined) ?? []);
	const chargerDeltaBundle = $derived((dashboard?.charger_delta_bundle as Record<string, unknown> | undefined) ?? null);
	const iterationTotals = $derived((stage3?.iteration_totals as Record<string, unknown>[] | undefined) ?? []);
	const mapData = $derived($appState.mapData);
	const mapSummary = $derived($appState.mapSummary);

	onMount(() => {
		appState.selectRun(runId);
		appState.ensureMapData();
	});

	const metrics = $derived([
		{ label: 'Iteration', value: currentState.current_iteration },
		{ label: 'Best Iteration', value: currentState.best_iteration },
		{ label: 'Best Worst Cost', value: currentState.best_worst_cost },
		{ label: 'Active Scopes', value: scopeIds.length }
	]);
</script>

<section class="panel stack">
	<div class="section-title"><h1>Stage 3: Reoptimization</h1></div>
	<MetricGrid items={metrics} />

	<div class="viz-grid">
		<TrendLineChart title="Worst Cost Over Iterations" points={trendSeries} />
		<ScenarioComparisonChart title="Baseline vs Best Iteration Scenarios" rows={scenarioComparison} />
	</div>

	<IterationCostEvolutionChart title="Per-Scenario Cost Across Iterations" iterationTotals={iterationTotals} />

	<StageSolutionMap
		title="Charger Changes vs Baseline"
		subtitle="Added, removed, and unchanged store chargers between baseline and best iteration"
		data={mapData}
		summary={mapSummary}
		bundle={chargerDeltaBundle}
		height={420}
	/>
</section>

<style>
	.viz-grid {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 1rem;
	}

	@media (max-width: 960px) {
		.viz-grid {
			grid-template-columns: 1fr;
		}
	}
</style>
