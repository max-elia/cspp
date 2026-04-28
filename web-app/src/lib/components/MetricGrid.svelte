<script lang="ts">
	import { formatNumber } from '$lib/format';

	type Metric = { label: string; value: unknown };
	let { items = [] } = $props<{ items?: Metric[] }>();
</script>

<div class="grid">
	{#each items as item}
		<div class="card">
			<div class="label">{item.label}</div>
			<div class="value">{formatNumber(item.value)}</div>
		</div>
	{/each}
</div>

<style>
	.grid {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(11rem, 1fr));
		gap: 0.9rem;
	}
	.card {
		border: 1px solid var(--border);
		padding: 1rem;
	}
	.label {
		font-size: 0.82rem;
		font-weight: 600;
		color: var(--muted);
		margin-bottom: 0.4rem;
	}
	.value {
		font-size: 1.35rem;
		font-weight: 600;
	}
</style>
