<script lang="ts">
	import { appState } from '$lib/app-state';
	import MetricGrid from '$lib/components/MetricGrid.svelte';
	import { page } from '$app/state';
	import { onMount } from 'svelte';

	const runId = $derived(page.params.runId ?? null);
	const scopeId = $derived(page.params.scopeId ?? '');

	onMount(() => {
		appState.selectRun(runId);
		appState.ensureStage3Scope(scopeId);
	});

	const detail = $derived($appState.details.stage3Scopes[scopeId] ?? null);
	const liveState = $derived((detail?.live_state as Record<string, unknown> | undefined) ?? {});
	const metrics = $derived([
		{ label: 'Runtime', value: liveState.runtime_sec },
		{ label: 'Best Obj', value: liveState.best_obj },
		{ label: 'Best Bound', value: liveState.best_bound },
		{ label: 'Gap', value: liveState.gap }
	]);
</script>

<section class="panel stack">
	<div class="section-title"><h1>Stage 3 Scope {scopeId}</h1></div>
	<MetricGrid items={metrics} />
</section>
