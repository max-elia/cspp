<script lang="ts">
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';

	onMount(() => {
		void goto('/instances', { replaceState: true });
	});
</script>

<section class="panel">
	<h1>Redirecting</h1>
</section>
