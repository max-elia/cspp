<script lang="ts">
	let { title = 'Raw Data', value = null } = $props<{ title?: string; value?: unknown }>();
</script>

<details class="panel">
	<summary>{title}</summary>
	<pre>{JSON.stringify(value, null, 2)}</pre>
</details>

<style>
	.panel {
		border: 1px solid var(--border);
		padding: 0.8rem 1rem;
	}
	summary {
		cursor: pointer;
		font-weight: 600;
	}
	pre {
		overflow: auto;
		font-size: 0.82rem;
		line-height: 1.5;
		margin: 1rem 0 0;
	}
</style>
