<script lang="ts">
	import { formatNumber } from '$lib/format';

	type ScenarioRow = Record<string, unknown>;

	let { title, scenarios = [], costKey = 'total_cost', idKey = 'scenario_id' } = $props<{
		title: string;
		scenarios?: ScenarioRow[];
		costKey?: string;
		idKey?: string;
	}>();

	function numeric(value: unknown): number | null {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? parsed : null;
	}

	type ValidRow = { id: string; cost: number };

	const validRows = $derived(
		scenarios
			.map((row: ScenarioRow) => ({ id: String(row[idKey] ?? 'n/a'), cost: numeric(row[costKey]) }))
			.filter((row: { id: string; cost: number | null }): row is ValidRow => row.cost !== null)
			.sort((a: ValidRow, b: ValidRow) => b.cost - a.cost)
	);

	const maxCost = $derived(validRows.length ? Math.max(...validRows.map((r: ValidRow) => r.cost)) : 1);
	const meanCost = $derived(validRows.length ? validRows.reduce((s: number, r: ValidRow) => s + r.cost, 0) / validRows.length : 0);
	const medianCost = $derived.by(() => {
		if (!validRows.length) return 0;
		const sorted = [...validRows].sort((a, b) => a.cost - b.cost);
		const mid = Math.floor(sorted.length / 2);
		return sorted.length % 2 ? sorted[mid].cost : (sorted[mid - 1].cost + sorted[mid].cost) / 2;
	});

	const width = 640;
	const height = $derived(Math.max(180, validRows.length * 22 + 60));
	const margin = { top: 28, right: 70, bottom: 30, left: 44 };
	const plotWidth = width - margin.left - margin.right;
	const plotHeight = $derived(height - margin.top - margin.bottom);
	const barHeight = $derived(validRows.length ? Math.min(16, (plotHeight - validRows.length * 2) / validRows.length) : 12);

	function xFor(cost: number): number {
		return margin.left + (cost / (maxCost || 1)) * plotWidth;
	}

	function yFor(index: number): number {
		if (!validRows.length) return margin.top;
		const totalBarSpace = validRows.length * barHeight + (validRows.length - 1) * 2;
		const offsetY = (plotHeight - totalBarSpace) / 2;
		return margin.top + offsetY + index * (barHeight + 2);
	}
</script>

<section class="chart-card">
	<h3>{title}</h3>
	{#if validRows.length}
		<svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={title}>
			<!-- mean line -->
			<line
				x1={xFor(meanCost)}
				y1={margin.top - 4}
				x2={xFor(meanCost)}
				y2={margin.top + plotHeight + 4}
				class="ref-line mean"
			/>
			<text x={xFor(meanCost)} y={margin.top - 8} text-anchor="middle" class="ref-label">Mean {formatNumber(meanCost, 0)}</text>

			<!-- median line -->
			<line
				x1={xFor(medianCost)}
				y1={margin.top - 4}
				x2={xFor(medianCost)}
				y2={margin.top + plotHeight + 4}
				class="ref-line median"
			/>
			<text x={xFor(medianCost)} y={height - 8} text-anchor="middle" class="ref-label">Median {formatNumber(medianCost, 0)}</text>

			{#each validRows as row, index}
				<rect
					x={margin.left}
					y={yFor(index)}
					width={Math.max(1, xFor(row.cost) - margin.left)}
					height={barHeight}
					class="bar"
					class:worst={index === 0}
					class:best={index === validRows.length - 1}
				/>
				<text x={margin.left - 6} y={yFor(index) + barHeight / 2 + 4} text-anchor="end" class="axis-label">
					S{row.id}
				</text>
				<text x={xFor(row.cost) + 4} y={yFor(index) + barHeight / 2 + 4} class="value-label">
					{formatNumber(row.cost, 0)}
				</text>
			{/each}
		</svg>
	{:else}
		<div class="empty-state">No cost data available.</div>
	{/if}
</section>

<style>
	.chart-card {
		border: 1px solid var(--border);
		background: #fff;
		padding: 0.9rem;
		display: grid;
		gap: 0.65rem;
	}

	h3 {
		margin: 0;
		font-size: 1rem;
	}

	svg {
		display: block;
		width: 100%;
		height: auto;
	}

	.bar {
		fill: #3b82f6;
	}

	.bar.worst {
		fill: #dc2626;
	}

	.bar.best {
		fill: #16a34a;
	}

	.axis-label {
		font-size: 11px;
		fill: #64748b;
	}

	.value-label {
		font-size: 10px;
		fill: #475569;
	}

	.ref-line {
		stroke-width: 1.5;
		stroke-dasharray: 5 3;
	}

	.ref-line.mean {
		stroke: #f59e0b;
	}

	.ref-line.median {
		stroke: #8b5cf6;
	}

	.ref-label {
		font-size: 10px;
		fill: #64748b;
	}

	.empty-state {
		border: 1px dashed var(--border);
		padding: 1rem;
		color: var(--muted);
	}
</style>
