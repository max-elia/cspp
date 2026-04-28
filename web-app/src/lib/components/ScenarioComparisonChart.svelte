<script lang="ts">
	import { formatNumber } from '$lib/format';

	type ScenarioRow = {
		scenario_id?: number | null;
		baseline_total?: number | null;
		best_total?: number | null;
		improvement?: number | null;
	};

	let { title, rows = [], maxBars = 12 } = $props<{ title: string; rows?: ScenarioRow[]; maxBars?: number }>();

	function numeric(value: unknown): number | null {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? parsed : null;
	}

	const orderedRows = $derived.by(() =>
		[...rows]
			.filter((row) => numeric(row.baseline_total) !== null || numeric(row.best_total) !== null)
			.sort((left, right) => (numeric(right.baseline_total) ?? 0) - (numeric(left.baseline_total) ?? 0))
			.slice(0, maxBars)
	);
	const maxValue = $derived(
		Math.max(
			1,
			...orderedRows.flatMap((row) => [numeric(row.baseline_total) ?? 0, numeric(row.best_total) ?? 0])
		)
	);
</script>

<section class="chart-card">
	<h3>{title}</h3>
	{#if orderedRows.length}
		<div class="bars">
			{#each orderedRows as row}
				<div class="bar-row">
					<div class="label">S{row.scenario_id ?? 'n/a'}</div>
					<div class="tracks">
						<div class="track">
							<div class="bar baseline" style={`width:${((numeric(row.baseline_total) ?? 0) / maxValue) * 100}%`}></div>
						</div>
						<div class="track">
							<div class="bar best" style={`width:${((numeric(row.best_total) ?? 0) / maxValue) * 100}%`}></div>
						</div>
					</div>
					<div class="values">
						<div>{formatNumber(row.baseline_total, 0)}</div>
						<div>{formatNumber(row.best_total, 0)}</div>
					</div>
				</div>
			{/each}
		</div>
		<div class="legend">
			<div><span class="swatch baseline"></span> Baseline</div>
			<div><span class="swatch best"></span> Best iteration</div>
		</div>
	{:else}
		<div class="empty-state">No comparison data available.</div>
	{/if}
</section>

<style>
	.chart-card {
		border: 1px solid var(--border);
		background: #fff;
		padding: 0.9rem;
		display: grid;
		gap: 0.7rem;
	}

	h3 {
		margin: 0;
		font-size: 1rem;
	}

	.bars {
		display: grid;
		gap: 0.5rem;
	}

	.bar-row {
		display: grid;
		grid-template-columns: 3rem minmax(0, 1fr) 7rem;
		gap: 0.75rem;
		align-items: center;
	}

	.label,
	.values {
		font-size: 0.78rem;
		color: var(--muted);
	}

	.values {
		display: grid;
		gap: 0.2rem;
		text-align: right;
	}

	.tracks {
		display: grid;
		gap: 0.18rem;
	}

	.track {
		height: 0.55rem;
		background: #f1f5f9;
		position: relative;
		overflow: hidden;
	}

	.bar {
		height: 100%;
	}

	.bar.baseline,
	.swatch.baseline {
		background: #94a3b8;
	}

	.bar.best,
	.swatch.best {
		background: #1d4ed8;
	}

	.legend {
		display: flex;
		flex-wrap: wrap;
		gap: 0.8rem;
		font-size: 0.82rem;
		color: var(--muted);
	}

	.legend div {
		display: inline-flex;
		align-items: center;
		gap: 0.45rem;
	}

	.swatch {
		width: 0.85rem;
		height: 0.55rem;
		display: inline-block;
	}

	.empty-state {
		border: 1px dashed var(--border);
		padding: 1rem;
		color: var(--muted);
	}
</style>
