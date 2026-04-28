<script lang="ts">
	import { appState } from '$lib/app-state';
	import CostDistributionChart from '$lib/components/CostDistributionChart.svelte';
	import MetricGrid from '$lib/components/MetricGrid.svelte';
	import StageSolutionMap from '$lib/components/StageSolutionMap.svelte';
	import { page } from '$app/state';
	import { onMount } from 'svelte';

	const runId = $derived(page.params.runId ?? null);
	const stage2 = $derived($appState.stage2 as Record<string, unknown> | null);
	const summary = $derived((stage2?.summary as Record<string, unknown> | undefined) ?? {});
	const scenarios = $derived((stage2?.scenarios as Record<string, unknown>[] | undefined) ?? []);
	const mapData = $derived($appState.mapData);
	const mapSummary = $derived($appState.mapSummary);
	let selectedScenarioId = $state<string>('');

	onMount(() => {
		appState.selectRun(runId);
		appState.ensureMapData();
	});

	const metrics = $derived([
		{ label: 'Scenarios', value: summary.total_scenarios },
		{ label: 'Completed', value: summary.completed_scenarios },
		{ label: 'Cluster Solves', value: summary.completed_cluster_solves },
		{ label: 'Clusters', value: summary.total_clusters }
	]);

	function costOf(row: Record<string, unknown>): number {
		const parsed = Number(row.total_cost);
		return Number.isFinite(parsed) ? parsed : -1;
	}

	const defaultScenarioId = $derived.by(() => {
		const viable = [...scenarios].filter((row) => row.total_cost !== null && row.total_cost !== undefined);
		const selected = viable.sort((left, right) => costOf(right) - costOf(left))[0] ?? scenarios[0];
		return selected ? String(selected.scenario_id ?? '') : '';
	});

	$effect(() => {
		if (!selectedScenarioId && defaultScenarioId) selectedScenarioId = defaultScenarioId;
	});

	$effect(() => {
		if (selectedScenarioId) appState.ensureStage2Scenario(selectedScenarioId);
	});

	const selectedDetail = $derived(selectedScenarioId ? $appState.details.stage2Scenarios[selectedScenarioId] ?? null : null);
	const selectedRouteBundle = $derived((selectedDetail?.route_bundle as Record<string, unknown> | undefined) ?? null);
	const selectedScenario = $derived(
		scenarios.find((row) => String(row.scenario_id ?? '') === selectedScenarioId) ?? null
	);
</script>

<section class="panel stack">
	<div class="section-title"><h1>Stage 2: Scenario Evaluation</h1></div>
	<MetricGrid items={metrics} />

	{#if scenarios.length}
		<CostDistributionChart title="Scenario Cost Distribution" scenarios={scenarios} costKey="total_cost" />
	{/if}

	{#if scenarios.length}
		<div class="selector-row">
			<label>
				<span>Scenario</span>
				<select bind:value={selectedScenarioId}>
					{#each scenarios as scenario}
						<option value={String(scenario.scenario_id ?? '')}>
							Scenario {String(scenario.scenario_id ?? 'n/a')}
						</option>
					{/each}
				</select>
			</label>
		</div>

		<StageSolutionMap
			title={`Scenario ${selectedScenarioId} Routes`}
			subtitle={`Status ${String(selectedScenario?.status ?? 'unknown')} · Cost ${String(selectedScenario?.total_cost ?? 'n/a')}`}
			data={mapData}
			summary={mapSummary}
			bundle={selectedRouteBundle}
			height={420}
		/>
	{/if}
</section>

<style>
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
</style>
