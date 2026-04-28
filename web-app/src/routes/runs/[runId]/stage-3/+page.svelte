<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { appState } from '$lib/app-state';
	import { onMount } from 'svelte';

	const runId = $derived(page.params.runId ?? null);

	onMount(() => {
		const state = $appState;
		if (!runId) { void goto('/instances', { replaceState: true }); return; }
		const catalog = state.catalog;
		let instanceId: string | null = null;
		for (const instance of catalog?.instances ?? []) {
			if ((instance.runs ?? []).some((r: { run_id: string }) => r.run_id === runId)) {
				instanceId = instance.instance_id;
				break;
			}
		}
		if (instanceId) {
			void goto(`/instances/${instanceId}/runs/${runId}/stage-3`, { replaceState: true });
		} else {
			void goto('/instances', { replaceState: true });
		}
	});
</script>

<section class="panel">
	<p class="muted">Redirecting...</p>
</section>
