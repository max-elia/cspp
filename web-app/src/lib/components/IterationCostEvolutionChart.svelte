<script lang="ts">
	import { scaleLinear } from 'd3';
	import { formatNumber } from '$lib/format';

	type IterationTotal = {
		iteration?: string | number | null;
		phase?: string | null;
		scenario?: string | number | null;
		total_cost?: string | number | null;
	};

	let { title, iterationTotals = [], maxLines = 40 } = $props<{
		title: string;
		iterationTotals?: IterationTotal[];
		maxLines?: number;
	}>();

	function numeric(value: unknown): number | null {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? parsed : null;
	}

	type ScenarioLine = {
		scenarioId: string;
		points: Array<{ iteration: number; cost: number }>;
		lastCost: number;
		isWorst: boolean;
		isBest: boolean;
	};

	const lines = $derived.by((): ScenarioLine[] => {
		// Group by scenario, only use 'applied' phase (final accepted state per iteration)
		const byScenario = new Map<string, Map<number, number>>();
		for (const row of iterationTotals) {
			if (String(row.phase ?? '') !== 'applied') continue;
			const cost = numeric(row.total_cost);
			const iteration = numeric(row.iteration);
			const scenarioId = String(row.scenario ?? '');
			if (cost === null || iteration === null || !scenarioId) continue;
			if (!byScenario.has(scenarioId)) byScenario.set(scenarioId, new Map());
			byScenario.get(scenarioId)!.set(iteration, cost);
		}

		const result: ScenarioLine[] = [];
		for (const [scenarioId, iterMap] of byScenario) {
			const points = [...iterMap.entries()]
				.map(([iteration, cost]) => ({ iteration, cost }))
				.sort((a, b) => a.iteration - b.iteration);
			if (!points.length) continue;
			result.push({
				scenarioId,
				points,
				lastCost: points[points.length - 1].cost,
				isWorst: false,
				isBest: false
			});
		}

		result.sort((a, b) => b.lastCost - a.lastCost);
		if (result.length) {
			result[0].isWorst = true;
			result[result.length - 1].isBest = true;
		}
		return result.slice(0, maxLines);
	});

	const width = 640;
	const height = 360;
	const margin = { top: 20, right: 60, bottom: 44, left: 62 };
	const plotWidth = width - margin.left - margin.right;
	const plotHeight = height - margin.top - margin.bottom;

	const allPoints = $derived(lines.flatMap((l) => l.points));
	const iterations = $derived([...new Set(allPoints.map((p) => p.iteration))].sort((a, b) => a - b));
	const costs = $derived(allPoints.map((p) => p.cost).filter((c) => c > 0));
	const costMin = $derived(costs.length ? Math.min(...costs) * 0.95 : 0);
	const costMax = $derived(costs.length ? Math.max(...costs) * 1.05 : 1);
	const iterMin = $derived(iterations.length ? Math.min(...iterations) : 0);
	const iterMax = $derived(iterations.length ? Math.max(...iterations) : 1);

	const xScale = $derived(
		scaleLinear()
			.domain([iterMin, Math.max(iterMax, iterMin + 1)])
			.range([margin.left, margin.left + plotWidth])
	);
	const yScale = $derived(
		scaleLinear()
			.domain([costMin, costMax])
			.range([margin.top + plotHeight, margin.top])
			.nice(5)
	);

	const xTicks = $derived(iterations.length <= 10 ? iterations : xScale.ticks(6).map(Math.round));
	const yTicks = $derived(yScale.ticks(5));

	function linePath(points: Array<{ iteration: number; cost: number }>): string {
		let path = '';
		for (const p of points) {
			const prefix = path ? 'L' : 'M';
			path += `${prefix}${xScale(p.iteration)},${yScale(p.cost)}`;
		}
		return path;
	}

	function lineColor(line: ScenarioLine): string {
		if (line.isWorst) return '#dc2626';
		if (line.isBest) return '#16a34a';
		return '#94a3b8';
	}

	function lineWidth(line: ScenarioLine): number {
		return line.isWorst || line.isBest ? 2.5 : 1;
	}

	function lineOpacity(line: ScenarioLine): number {
		return line.isWorst || line.isBest ? 1 : 0.4;
	}
</script>

<section class="chart-card">
	<h3>{title}</h3>
	{#if lines.length}
		<div class="chart-frame">
			<svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={title}>
				{#each yTicks as tick}
					<line x1={margin.left} y1={yScale(tick)} x2={margin.left + plotWidth} y2={yScale(tick)} class="grid-line" />
					<text x={margin.left - 8} y={yScale(tick) + 4} text-anchor="end" class="axis-label">{formatNumber(tick, 0)}</text>
				{/each}

				{#each xTicks as tick}
					<line x1={xScale(tick)} y1={margin.top} x2={xScale(tick)} y2={margin.top + plotHeight} class="grid-line" />
					<text x={xScale(tick)} y={margin.top + plotHeight + 18} text-anchor="middle" class="axis-label">I{tick}</text>
				{/each}

				<!-- background lines first -->
				{#each lines.filter((l) => !l.isWorst && !l.isBest) as line}
					<path d={linePath(line.points)} fill="none" stroke={lineColor(line)} stroke-width={lineWidth(line)} opacity={lineOpacity(line)} />
				{/each}

				<!-- highlighted lines on top -->
				{#each lines.filter((l) => l.isWorst || l.isBest) as line}
					<path d={linePath(line.points)} fill="none" stroke={lineColor(line)} stroke-width={lineWidth(line)} opacity={lineOpacity(line)} />
					{#if line.points.length}
						<text
							x={xScale(line.points[line.points.length - 1].iteration) + 6}
							y={yScale(line.points[line.points.length - 1].cost) + 4}
							class="end-label"
							fill={lineColor(line)}
						>S{line.scenarioId}</text>
					{/if}
				{/each}

				<text x={margin.left + plotWidth / 2} y={height - 6} text-anchor="middle" class="axis-title">Iteration</text>
				<text
					x={16} y={margin.top + plotHeight / 2}
					transform={`rotate(-90 16 ${margin.top + plotHeight / 2})`}
					text-anchor="middle" class="axis-title"
				>Cost</text>
			</svg>
		</div>

		<div class="legend">
			<div><span class="swatch" style="background:#dc2626"></span> Worst scenario</div>
			<div><span class="swatch" style="background:#16a34a"></span> Best scenario</div>
			<div><span class="swatch" style="background:#94a3b8"></span> Other scenarios</div>
		</div>
	{:else}
		<div class="empty-state">No iteration cost data available.</div>
	{/if}
</section>

<style>
	.chart-card {
		border: 1px solid var(--border);
		padding: 0.85rem;
		background: #fff;
	}

	h3 {
		margin: 0;
		font-size: 1rem;
	}

	.chart-frame {
		position: relative;
		margin-top: 0.5rem;
		aspect-ratio: 16 / 9;
	}

	svg {
		display: block;
		width: 100%;
		height: 100%;
	}

	.grid-line {
		stroke: #d8dde3;
		stroke-width: 1;
	}

	.axis-label {
		font-size: 11px;
		fill: var(--muted);
	}

	.axis-title {
		font-size: 12px;
		font-weight: 600;
		fill: var(--text);
	}

	.end-label {
		font-size: 10px;
		font-weight: 600;
	}

	.legend {
		display: flex;
		flex-wrap: wrap;
		gap: 0.8rem;
		font-size: 0.82rem;
		color: var(--muted);
		margin-top: 0.5rem;
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

	.empty-state {
		aspect-ratio: 16 / 9;
		display: flex;
		align-items: center;
		justify-content: center;
		color: var(--muted);
		font-size: 0.9rem;
		background: #f8f9fb;
		border: 1px dashed var(--border);
		margin-top: 0.5rem;
	}
</style>
