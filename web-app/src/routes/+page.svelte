<script lang="ts">
	import { appState } from '$lib/app-state';
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';

	onMount(() => {
		const unsubscribe = appState.subscribe((state) => {
			const instanceId = state.selectedInstanceId ?? state.catalog?.latest_instance_id ?? null;
			if (instanceId) {
				void goto(`/instances/${instanceId}`, { replaceState: true });
			}
		});
		return unsubscribe;
	});
</script>

<section class="panel">
	<h1>Waiting For Mirrored Files</h1>
	<p>The app redirects to the latest instance as soon as `exports/state/web_app_catalog.json` has been synced into IndexedDB.</p>
	<p>Use the instances page to create or inspect grouped solve runs.</p>
</section>
