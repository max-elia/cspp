<script lang="ts">
	import { appState } from '$lib/app-state';
	import MetricGrid from '$lib/components/MetricGrid.svelte';
	import SimpleMap from '$lib/components/SimpleMap.svelte';
	import { page } from '$app/state';
	import { onMount } from 'svelte';

	const runId = $derived(page.params.runId ?? null);
	const clusterId = $derived(page.params.clusterId ?? '');

	onMount(() => {
		appState.selectRun(runId);
		appState.ensureStage3Cluster(clusterId);
	});

	const detail = $derived($appState.details.stage3Clusters[clusterId] ?? null);
	const liveState = $derived((detail?.live_state as Record<string, unknown> | undefined) ?? {});
	const metrics = $derived([
		{ label: 'Iteration', value: liveState.current_iteration },
		{ label: 'Phase', value: liveState.current_phase },
		{ label: 'Elapsed', value: liveState.elapsed_sec },
		{ label: 'Status', value: liveState.status }
	]);
</script>

<section class="panel stack">
	<div class="section-title"><h1>Stage 3 Cluster {clusterId}</h1></div>
	<MetricGrid items={metrics} />
	<SimpleMap data={(detail?.map as import('$lib/types').FeatureCollection | undefined) ?? null} height={320} />
</section>
