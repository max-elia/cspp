<script lang="ts">
	import './layout.css';
	import { appState } from '$lib/app-state';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import type { FrontendManifest } from '$lib/types';
	import { onMount } from 'svelte';

	let { children } = $props();

	onMount(() => {
		appState.start();
		return () => appState.stop();
	});

	const catalog = $derived($appState.catalog);
	const instances = $derived(catalog?.instances ?? []);

	// Derive context from URL params first, then app state
	const instanceId = $derived(page.params.instanceId ?? $appState.selectedInstanceId ?? null);
	const runId = $derived(page.params.runId ?? $appState.selectedRunId ?? null);

	const currentPath = $derived(page.url.pathname);
	const manifest = $derived(($appState.manifest as FrontendManifest | null) ?? null);
	const manifestCounts = $derived((manifest?.counts as Record<string, number> | undefined) ?? {});
	const manifestRoutes = $derived((manifest?.available_routes as Record<string, unknown> | undefined) ?? {});
	const overview = $derived(($appState.overview as Record<string, unknown> | null) ?? null);
	const stageStatus = $derived((overview?.stage_status as Record<string, unknown> | undefined) ?? {});

	const hasMapData = $derived.by(() => {
		const routeValue = manifestRoutes.map;
		if (typeof routeValue === 'boolean') return routeValue;
		return Boolean(runId);
	});
	const hasStage1Data = $derived.by(() => Number(manifestCounts.stage_1_clusters ?? 0) > 0);
	const hasStage2Data = $derived.by(() => Number(manifestCounts.stage_2_scenarios ?? 0) > 0);
	const hasStage4Data = $derived.by(() => {
		const clusterCount = Number(manifestCounts.stage_3_clusters ?? 0);
		const scopeCount = Number(manifestCounts.stage_3_scopes ?? 0);
		return clusterCount > 0 || scopeCount > 0;
	});
	const currentInstance = $derived.by(() => instances.find((instance) => instance.instance_id === instanceId) ?? null);
	const currentRuns = $derived(currentInstance?.runs ?? []);
	const selectedRun = $derived.by(() => currentRuns.find((run) => run.run_id === runId) ?? null);

	function isActive(href: string, exact = false): boolean {
		if (exact) return currentPath === href;
		return currentPath === href || currentPath.startsWith(`${href}/`);
	}

	async function onInstanceChange(event: Event): Promise<void> {
		const target = event.currentTarget as HTMLSelectElement;
		const id = target.value || null;
		appState.selectInstance(id);
		if (id) await goto(`/instances/${id}`);
		else await goto('/instances');
	}

	// Base paths for nested nav
	const instanceBase = $derived(instanceId ? `/instances/${instanceId}` : null);
	const runBase = $derived(instanceId && runId ? `/instances/${instanceId}/runs/${runId}` : null);

	const visibleError = $derived.by(() => {
		const first = $appState.errors[0];
		if (!first) return null;
		const text = String(first).trim();
		if (!text) return null;
		// Suppress transient sync warnings that surface during normal polling.
		const lower = text.toLowerCase();
		if (lower.includes('sync') && (lower.includes('pending') || lower.includes('retry'))) return null;
		return text;
	});
</script>

<svelte:head>
	<title>CSPP Live Dashboard</title>
</svelte:head>

<div class="workbench-shell">
	<aside class="context-rail" aria-label="Workbench context">
		<div class="rail-brand">
			<a class="brand" href="/instances">CSPP Monitor</a>
		</div>

		<nav class="rail-section" aria-label="Global">
			<a href="/instances" class:active={isActive('/instances', true)} aria-current={isActive('/instances', true) ? 'page' : undefined}>Instances</a>
			<a href="/instances/new" class:active={isActive('/instances/new')} aria-current={isActive('/instances/new') ? 'page' : undefined}>New Instance</a>
		</nav>

		<label class="run-select">
			<span>Selected Instance</span>
			<select onchange={onInstanceChange} value={instanceId ?? ''}>
				<option value="">No instance</option>
				{#each instances as instance}
					<option value={instance.instance_id}>{instance.label ?? instance.instance_id}</option>
				{/each}
			</select>
		</label>

		{#if instanceBase}
			<nav class="rail-section" aria-label="Instance views">
				<div class="rail-heading">Instance</div>
				<a href={instanceBase} class:active={isActive(instanceBase, true)} aria-current={isActive(instanceBase, true) ? 'page' : undefined}>Overview + Map</a>
				<a href={`/instances/new?source=${encodeURIComponent(instanceId ?? '')}&step=stores`} class:active={isActive('/instances/new')}>Derive Instance</a>
				<a href={`${instanceBase}/runs`} class:active={isActive(`${instanceBase}/runs`, !runId)} aria-current={isActive(`${instanceBase}/runs`, !runId) ? 'page' : undefined}>Runs</a>
			</nav>

			<div class="rail-section">
				<div class="rail-heading">Runs</div>
				{#if currentRuns.length}
					<div class="rail-run-list">
						{#each currentRuns.slice(0, 8) as run}
							<a class="rail-run" class:active={run.run_id === runId} href={`${instanceBase}/runs/${run.run_id}`}>
								<span>{run.run_id}</span>
								<StatusPill value={run.status ?? 'idle'} />
							</a>
						{/each}
					</div>
				{:else}
					<div class="empty-rail">No runs yet</div>
				{/if}
			</div>
		{/if}

		{#if runBase}
			<nav class="rail-section" aria-label="Run views">
				<div class="rail-heading">Selected Run</div>
				{#if selectedRun}
					<div class="rail-run-title">{selectedRun.run_id}</div>
				{/if}
				<a href={runBase} class:active={isActive(runBase, true)} aria-current={isActive(runBase, true) ? 'page' : undefined}>Progress</a>
				{#if hasMapData}
					<a href={`${runBase}/map`} class:active={isActive(`${runBase}/map`)} aria-current={isActive(`${runBase}/map`) ? 'page' : undefined}>Map</a>
				{/if}
				{#if hasStage1Data}
					<a href={`${runBase}/stage-1`} class:active={isActive(`${runBase}/stage-1`)} aria-current={isActive(`${runBase}/stage-1`) ? 'page' : undefined}>Stage 1: Cluster Solve</a>
				{/if}
				{#if hasStage2Data}
					<a href={`${runBase}/stage-2`} class:active={isActive(`${runBase}/stage-2`)} aria-current={isActive(`${runBase}/stage-2`) ? 'page' : undefined}>Stage 2: Scenarios</a>
				{/if}
				{#if hasStage4Data}
					<a href={`${runBase}/stage-3`} class:active={isActive(`${runBase}/stage-3`)} aria-current={isActive(`${runBase}/stage-3`) ? 'page' : undefined}>Stage 3: Reoptimization</a>
				{/if}
			</nav>
		{/if}
	</aside>

	<div class="main-shell">
		{#if visibleError}
			<div class="error-banner">{visibleError}</div>
		{/if}

		<main class="page">
			{@render children()}
		</main>
	</div>
</div>
