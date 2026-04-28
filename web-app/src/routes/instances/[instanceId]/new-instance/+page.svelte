<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { onMount } from 'svelte';

	onMount(() => {
		const instanceId = page.params.instanceId;
		// Redirect to top-level new instance workflow, passing source instance as query param
		void goto(`/instances/new${instanceId ? `?source=${encodeURIComponent(instanceId)}` : ''}`, { replaceState: true });
	});
</script>

<section class="panel">
	<p class="muted">Redirecting...</p>
</section>
