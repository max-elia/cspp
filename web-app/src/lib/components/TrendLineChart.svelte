<script lang="ts">
	import { formatNumber } from '$lib/format';

	type TrendPoint = {
		iteration?: number | null;
		label?: string | null;
		worst_cost?: number | null;
		best_worst_cost?: number | null;
		accepted?: boolean | null;
	};

	let { title, points = [] } = $props<{ title: string; points?: TrendPoint[] }>();

	const width = 640;
	const height = 280;
	const margin = { top: 24, right: 24, bottom: 40, left: 56 };
	const plotWidth = width - margin.left - margin.right;
	const plotHeight = height - margin.top - margin.bottom;

	function numeric(value: unknown): number | null {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? parsed : null;
	}

	const validPoints = $derived(
		points.filter((point: TrendPoint) => numeric(point.worst_cost) !== null || numeric(point.best_worst_cost) !== null)
	);
	const values = $derived(
		validPoints.flatMap((point: TrendPoint) =>
			[numeric(point.worst_cost), numeric(point.best_worst_cost)].filter((value): value is number => value !== null)
		)
	);
	const yMin = $derived(values.length ? Math.min(...values) : 0);
	const yMax = $derived(values.length ? Math.max(...values) : 1);
	const ySpan = $derived(Math.max(yMax - yMin, Math.abs(yMax) * 0.05, 1));
	const ticks = $derived.by(() => {
		const items: number[] = [];
		for (let index = 0; index < 5; index += 1) {
			items.push(yMin + (ySpan * index) / 4);
		}
		return items;
	});

	function xFor(index: number): number {
		if (validPoints.length <= 1) return margin.left + plotWidth / 2;
		return margin.left + (index / (validPoints.length - 1)) * plotWidth;
	}

	function yFor(value: number): number {
		const denominator = ySpan || 1;
		return margin.top + plotHeight - ((value - yMin) / denominator) * plotHeight;
	}

	function linePath(key: 'worst_cost' | 'best_worst_cost'): string {
		let path = '';
		validPoints.forEach((point: TrendPoint, index: number) => {
			const value = numeric(point[key]);
			if (value === null) return;
			const prefix = path ? 'L' : 'M';
			path += `${prefix}${xFor(index)},${yFor(value)}`;
		});
		return path;
	}
</script>

<section class="chart-card">
	<h3>{title}</h3>
	{#if validPoints.length}
		<svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={title}>
			{#each ticks as tick}
				<line x1={margin.left} y1={yFor(tick)} x2={margin.left + plotWidth} y2={yFor(tick)} class="grid" />
				<text x={margin.left - 8} y={yFor(tick) + 4} text-anchor="end" class="axis">{formatNumber(tick, 0)}</text>
			{/each}

			<path d={linePath('worst_cost')} class="line worst" />
			<path d={linePath('best_worst_cost')} class="line best" />

			{#each validPoints as point, index}
				{#if numeric(point.worst_cost) !== null}
					<circle cx={xFor(index)} cy={yFor(numeric(point.worst_cost)!)} r="4" class="dot worst" />
				{/if}
				{#if numeric(point.best_worst_cost) !== null}
					<circle cx={xFor(index)} cy={yFor(numeric(point.best_worst_cost)!)} r="3.5" class="dot best" />
				{/if}
				<text x={xFor(index)} y={height - 10} text-anchor="middle" class="axis">{point.label ?? `I${point.iteration ?? index}`}</text>
			{/each}
		</svg>
		<div class="legend">
			<div><span class="swatch worst"></span> Iteration worst cost</div>
			<div><span class="swatch best"></span> Best-so-far worst cost</div>
		</div>
	{:else}
		<div class="empty-state">No trend data available.</div>
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

	.grid {
		stroke: #e2e8f0;
		stroke-width: 1;
	}

	.axis {
		fill: #64748b;
		font-size: 11px;
	}

	.line {
		fill: none;
		stroke-width: 2.5;
	}

	.line.worst,
	.dot.worst {
		stroke: #dc2626;
		fill: #dc2626;
	}

	.line.best,
	.dot.best {
		stroke: #1d4ed8;
		fill: #1d4ed8;
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
		height: 0.2rem;
		display: inline-block;
	}

	.swatch.worst {
		background: #dc2626;
	}

	.swatch.best {
		background: #1d4ed8;
	}

	.empty-state {
		border: 1px dashed var(--border);
		padding: 1rem;
		color: var(--muted);
	}
</style>
