<script lang="ts">
	import { line as d3Line, scaleLinear, scaleLog } from 'd3';
	import { formatDuration, formatNumber } from '$lib/format';

	type Point = { x: number; y: number };
	type TrajectorySeries = {
		iteration: number;
		isCurrent: boolean;
		points: Point[];
	};
	type HoverPoint = {
		iteration: number;
		isCurrent: boolean;
		point: Point;
		sx: number;
		sy: number;
	};
	type DerivedPoint = Point & { iteration: number; isCurrent: boolean };

	let {
		title,
		subtitle = '',
		series = [],
		xLabel = 'Runtime',
		yLabel = 'Objective',
		yScaleType = 'linear',
		onclick = undefined
	} = $props<{
		title: string;
		subtitle?: string;
		series?: TrajectorySeries[];
		xLabel?: string;
		yLabel?: string;
		yScaleType?: 'linear' | 'log';
		onclick?: (() => void) | undefined;
	}>();

	const width = 640;
	const height = 360;
	const margin = { top: 20, right: 20, bottom: 40, left: 62 };
	const plotWidth = width - margin.left - margin.right;
	const plotHeight = height - margin.top - margin.bottom;

	let hoverPoint = $state<HoverPoint | null>(null);
	let hoverX = $state(0);
	let hoverY = $state(0);

	const allPoints = $derived(
		series.flatMap((trajectory: TrajectorySeries) =>
			trajectory.points.map(
				(point: Point): DerivedPoint => ({ ...point, iteration: trajectory.iteration, isCurrent: trajectory.isCurrent })
			)
		)
	);
	const hasData = $derived(allPoints.length > 0);
	const xMax = $derived(hasData ? Math.max(...allPoints.map((point: DerivedPoint) => point.x), 1) : 1);
	const positivePoints = $derived(allPoints.filter((point: DerivedPoint) => point.y > 0));
	const hasPositiveData = $derived(positivePoints.length > 0);
	const yMin = $derived(hasData ? Math.min(...allPoints.map((point: DerivedPoint) => point.y)) : 0);
	const yMax = $derived(hasData ? Math.max(...allPoints.map((point: DerivedPoint) => point.y)) : 1);
	const yPositiveMin = $derived(hasPositiveData ? Math.min(...positivePoints.map((point: DerivedPoint) => point.y)) : 1);
	const yPositiveMax = $derived(hasPositiveData ? Math.max(...positivePoints.map((point: DerivedPoint) => point.y)) : 10);
	const ySpan = $derived(Math.max(yMax - yMin, Math.abs(yMax) * 0.05, 1));
	const yDomainMin = $derived(yMin - ySpan * 0.08);
	const yDomainMax = $derived(yMax + ySpan * 0.08);
	const yLogDomainMin = $derived(yPositiveMin > 0 ? yPositiveMin / Math.pow(10, 0.05) : 1);
	const yLogDomainMax = $derived(
		yPositiveMax > yPositiveMin ? yPositiveMax * Math.pow(10, 0.05) : yPositiveMin * 10
	);

	const xScale = $derived(
		scaleLinear()
			.domain([0, xMax])
			.range([margin.left, margin.left + plotWidth])
	);
	const yScale = $derived(
		yScaleType === 'log'
			? scaleLog()
					.domain([yLogDomainMin, yLogDomainMax])
					.range([margin.top + plotHeight, margin.top])
			: scaleLinear()
					.domain([yDomainMin, yDomainMax])
					.range([margin.top + plotHeight, margin.top])
					.nice(5)
	);
	const xTicks = $derived(xScale.ticks(5));
	const yTicks = $derived.by(() => {
		if (yScaleType !== 'log') return yScale.ticks(5);
		// For narrow log ranges (< one decade), generate ~5 nice ticks linearly within the data
		// extent rather than using d3's decade-aligned defaults, which would only place 1-2 ticks.
		const lo = yLogDomainMin;
		const hi = yLogDomainMax;
		if (hi <= lo) return [lo];
		const decades = Math.log10(hi / lo);
		if (decades >= 1) {
			return yScale.ticks().filter((tick) => tick >= lo && tick <= hi);
		}
		const step = (hi - lo) / 5;
		const magnitude = Math.pow(10, Math.floor(Math.log10(step)));
		const niceStep = Math.ceil(step / magnitude) * magnitude;
		const start = Math.ceil(lo / niceStep) * niceStep;
		const out: number[] = [];
		for (let v = start; v <= hi; v += niceStep) out.push(Number(v.toPrecision(6)));
		return out.length ? out : [lo, hi];
	});
	const lineGenerator = $derived(
		d3Line<Point>()
			.x((point: Point) => xScale(point.x))
			.y((point: Point) => yScale(point.y))
	);

	const plottedSeries = $derived(
		series.map((trajectory: TrajectorySeries) => {
			const points = yScaleType === 'log' ? trajectory.points.filter((point: Point) => point.y > 0) : trajectory.points;
			return {
				...trajectory,
				points,
				path: lineGenerator(points) ?? '',
				lastPoint: points.length ? points[points.length - 1] : null
			};
		})
	);

	const hoverCandidates = $derived(
		series.flatMap((trajectory: TrajectorySeries) =>
			trajectory.points
				.filter((point: Point) => yScaleType !== 'log' || point.y > 0)
				.map((point: Point): HoverPoint => ({
				iteration: trajectory.iteration,
				isCurrent: trajectory.isCurrent,
				point,
				sx: xScale(point.x),
				sy: yScale(point.y)
				}))
		)
	);

	function clearHover(): void {
		hoverPoint = null;
	}

	function handlePointerMove(event: PointerEvent): void {
		const target = event.currentTarget;
		if (!(target instanceof SVGRectElement)) return;

		const bounds = target.getBoundingClientRect();
		const sx = ((event.clientX - bounds.left) / bounds.width) * width;
		const sy = ((event.clientY - bounds.top) / bounds.height) * height;

		let nearest: HoverPoint | null = null;
		let nearestDistance = Number.POSITIVE_INFINITY;
		for (const candidate of hoverCandidates) {
			const dx = candidate.sx - sx;
			const dy = candidate.sy - sy;
			const distance = dx * dx + dy * dy;
			if (distance < nearestDistance) {
				nearest = candidate;
				nearestDistance = distance;
			}
		}

		hoverPoint = nearest;
		if (nearest) {
			hoverX = nearest.sx;
			hoverY = nearest.sy;
		}
	}
</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<!-- svelte-ignore a11y_click_events_have_key_events -->
<section class="chart-card" class:clickable={!!onclick} onclick={onclick}>
	<div class="chart-header">
		<div>
			<h3>{title}</h3>
			{#if subtitle}
				<p>{subtitle}</p>
			{/if}
		</div>
	</div>

	{#if hasData}
		<div class="chart-frame">
			<svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${title} objective trajectory`}>
				{#each yTicks as tick}
					<line
						x1={margin.left}
						y1={yScale(tick)}
						x2={margin.left + plotWidth}
						y2={yScale(tick)}
						class="grid-line"
					/>
					<text x={margin.left - 8} y={yScale(tick) + 4} class="axis-label axis-y">{formatNumber(tick, 0)}</text>
				{/each}

				{#each xTicks as tick}
					<line
						x1={xScale(tick)}
						y1={margin.top}
						x2={xScale(tick)}
						y2={margin.top + plotHeight}
						class="grid-line"
					/>
					<text x={xScale(tick)} y={margin.top + plotHeight + 18} text-anchor="middle" class="axis-label">
						{formatDuration(Math.round(tick))}
					</text>
				{/each}

				{#each plottedSeries as trajectory}
					<path
						d={trajectory.path}
						class:current={trajectory.isCurrent}
						class:previous={!trajectory.isCurrent}
						class="trajectory"
					/>
					{#if trajectory.lastPoint}
						<circle
							cx={xScale(trajectory.lastPoint.x)}
							cy={yScale(trajectory.lastPoint.y)}
							r={trajectory.isCurrent ? 4 : 3}
							class:current-point={trajectory.isCurrent}
							class:previous-point={!trajectory.isCurrent}
						/>
					{/if}
				{/each}

				{#if hoverPoint}
					<line x1={hoverX} y1={margin.top} x2={hoverX} y2={margin.top + plotHeight} class="hover-line" />
					<circle cx={hoverX} cy={hoverY} r={5} class="hover-point" />
				{/if}

				<!-- svelte-ignore a11y_no_static_element_interactions -->
				<rect
					x={margin.left}
					y={margin.top}
					width={plotWidth}
					height={plotHeight}
					class="hover-overlay"
					onpointermove={handlePointerMove}
					onpointerleave={clearHover}
				/>

				<text x={margin.left + plotWidth / 2} y={height - 6} text-anchor="middle" class="axis-title">{xLabel}</text>
				<text
					x={16}
					y={margin.top + plotHeight / 2}
					transform={`rotate(-90 16 ${margin.top + plotHeight / 2})`}
					text-anchor="middle"
					class="axis-title"
				>
					{yLabel}
				</text>
			</svg>

			{#if hoverPoint}
				<div
					class="tooltip"
					style={`left: ${Math.min(Math.max((hoverX / width) * 100, 12), 88)}%; top: ${Math.min(Math.max((hoverY / height) * 100, 14), 82)}%;`}
				>
					<div>Iteration {hoverPoint.iteration}</div>
					<div>{hoverPoint.isCurrent ? 'Current iteration' : 'Previous iteration'}</div>
					<div>Runtime: {formatDuration(Math.round(hoverPoint.point.x))}</div>
					<div>Objective: {formatNumber(hoverPoint.point.y, 2)}</div>
				</div>
			{/if}
		</div>
	{:else}
		<div class="empty-state">No objective trajectory yet.</div>
	{/if}
</section>

<style>
	.chart-card {
		border: 1px solid var(--border);
		padding: 0.85rem;
		background: #fff;
	}

	.chart-card.clickable {
		cursor: pointer;
	}

	.chart-card.clickable:hover {
		border-color: #1c58a1;
	}

	.chart-header h3 {
		margin: 0;
		font-size: 0.98rem;
	}

	.chart-header p {
		margin: 0.25rem 0 0;
		font-size: 0.82rem;
		color: var(--muted);
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

	.axis-y {
		text-anchor: end;
	}

	.axis-title {
		font-size: 12px;
		font-weight: 600;
		fill: var(--text);
	}

	.trajectory {
		fill: none;
		stroke-linecap: round;
		stroke-linejoin: round;
	}

	.trajectory.previous {
		stroke: rgba(28, 88, 161, 0.22);
		stroke-width: 2;
	}

	.trajectory.current {
		stroke: #1c58a1;
		stroke-width: 3;
	}

	circle.previous-point {
		fill: rgba(28, 88, 161, 0.35);
	}

	circle.current-point {
		fill: #1c58a1;
	}

	.hover-overlay {
		fill: transparent;
		pointer-events: all;
	}

	.hover-line {
		stroke: rgba(28, 88, 161, 0.45);
		stroke-width: 1.5;
		stroke-dasharray: 4 3;
	}

	.hover-point {
		fill: #c44b23;
		stroke: #fff;
		stroke-width: 2;
	}

	.tooltip {
		position: absolute;
		transform: translate(-50%, calc(-100% - 10px));
		pointer-events: none;
		background: rgba(18, 26, 36, 0.94);
		color: #fff;
		padding: 0.55rem 0.7rem;
		font-size: 0.78rem;
		line-height: 1.35;
		white-space: nowrap;
		box-shadow: 0 10px 30px rgba(18, 26, 36, 0.18);
	}

	.empty-state {
		aspect-ratio: 16 / 9;
		display: flex;
		align-items: center;
		justify-content: center;
		text-align: center;
		color: var(--muted);
		font-size: 0.9rem;
		background: #f8f9fb;
		border: 1px dashed var(--border);
		margin-top: 0.5rem;
	}
</style>
