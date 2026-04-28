<script lang="ts">
	import { scaleLinear, scaleSqrt } from 'd3';
	import { formatDuration, formatNumber } from '$lib/format';

	type ClusterRow = Record<string, unknown>;
	type HoverInfo = { cluster: ClusterRow; sx: number; sy: number } | null;

	let { title, clusters = [] } = $props<{ title: string; clusters?: ClusterRow[] }>();

	let hoverInfo = $state<HoverInfo>(null);

	function numeric(value: unknown): number | null {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? parsed : null;
	}

	const validClusters = $derived(
		clusters.filter(
			(c: ClusterRow) => numeric(c.runtime_sec) !== null && numeric(c.customers) !== null
		)
	);

	const width = 640;
	const height = 360;
	const margin = { top: 20, right: 24, bottom: 44, left: 62 };
	const plotWidth = width - margin.left - margin.right;
	const plotHeight = height - margin.top - margin.bottom;

	const customerValues = $derived(validClusters.map((c: ClusterRow) => numeric(c.customers)!));
	const runtimeValues = $derived(validClusters.map((c: ClusterRow) => numeric(c.runtime_sec)!));

	const xMax = $derived(customerValues.length ? Math.max(...customerValues) * 1.1 : 10);
	const yMax = $derived(runtimeValues.length ? Math.max(...runtimeValues) * 1.1 : 100);

	const xScale = $derived(scaleLinear().domain([0, xMax]).range([margin.left, margin.left + plotWidth]));
	const yScale = $derived(scaleLinear().domain([0, yMax]).range([margin.top + plotHeight, margin.top]));
	const rScale = $derived(
		scaleSqrt()
			.domain([0, Math.max(1, ...validClusters.map((c: ClusterRow) => numeric(c.iterations) ?? 1))])
			.range([4, 16])
	);

	const xTicks = $derived(xScale.ticks(6));
	const yTicks = $derived(yScale.ticks(5));

	function statusColor(status: unknown): string {
		const s = String(status ?? '').toLowerCase();
		if (s === 'solved' || s === 'completed') return '#16a34a';
		if (s === 'timeout') return '#dc2626';
		if (s === 'running') return '#3b82f6';
		return '#94a3b8';
	}

	function handlePointerMove(event: PointerEvent): void {
		const target = event.currentTarget;
		if (!(target instanceof SVGRectElement)) return;
		const bounds = target.getBoundingClientRect();
		const sx = ((event.clientX - bounds.left) / bounds.width) * width;
		const sy = ((event.clientY - bounds.top) / bounds.height) * height;

		let nearest: HoverInfo = null;
		let nearestDist = Infinity;
		for (const c of validClusters) {
			const cx = xScale(numeric(c.customers)!);
			const cy = yScale(numeric(c.runtime_sec)!);
			const d = (cx - sx) ** 2 + (cy - sy) ** 2;
			if (d < nearestDist) {
				nearestDist = d;
				nearest = { cluster: c, sx: cx, sy: cy };
			}
		}
		hoverInfo = nearest;
	}
</script>

<section class="chart-card">
	<h3>{title}</h3>
	{#if validClusters.length}
		<div class="chart-frame">
			<svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={title}>
				{#each yTicks as tick}
					<line x1={margin.left} y1={yScale(tick)} x2={margin.left + plotWidth} y2={yScale(tick)} class="grid-line" />
					<text x={margin.left - 8} y={yScale(tick) + 4} text-anchor="end" class="axis-label">{formatDuration(Math.round(tick))}</text>
				{/each}

				{#each xTicks as tick}
					<line x1={xScale(tick)} y1={margin.top} x2={xScale(tick)} y2={margin.top + plotHeight} class="grid-line" />
					<text x={xScale(tick)} y={margin.top + plotHeight + 18} text-anchor="middle" class="axis-label">{Math.round(tick)}</text>
				{/each}

				{#each validClusters as c}
					<circle
						cx={xScale(numeric(c.customers)!)}
						cy={yScale(numeric(c.runtime_sec)!)}
						r={rScale(numeric(c.iterations) ?? 1)}
						fill={statusColor(c.status)}
						opacity={0.75}
						stroke="#fff"
						stroke-width={1.5}
					/>
				{/each}

				{#if hoverInfo}
					<circle cx={hoverInfo.sx} cy={hoverInfo.sy} r={6} class="hover-ring" />
				{/if}

				<rect
					x={margin.left} y={margin.top} width={plotWidth} height={plotHeight}
					class="hover-overlay"
					role="presentation"
					onpointermove={handlePointerMove}
					onpointerleave={() => (hoverInfo = null)}
				/>

				<text x={margin.left + plotWidth / 2} y={height - 6} text-anchor="middle" class="axis-title">Stores</text>
				<text
					x={16} y={margin.top + plotHeight / 2}
					transform={`rotate(-90 16 ${margin.top + plotHeight / 2})`}
					text-anchor="middle" class="axis-title"
				>Runtime</text>
			</svg>

			{#if hoverInfo}
				<div
					class="tooltip"
					style={`left: ${Math.min(Math.max((hoverInfo.sx / width) * 100, 15), 85)}%; top: ${Math.min(Math.max((hoverInfo.sy / height) * 100, 14), 82)}%;`}
				>
					<div>Cluster {hoverInfo.cluster.cluster_id}</div>
					<div>Stores: {hoverInfo.cluster.customers}</div>
					<div>Runtime: {formatDuration(Math.round(numeric(hoverInfo.cluster.runtime_sec)!))}</div>
					<div>Iterations: {hoverInfo.cluster.iterations ?? 'n/a'}</div>
					<div>Gap: {numeric(hoverInfo.cluster.reached_gap) !== null ? formatNumber(numeric(hoverInfo.cluster.reached_gap)! * 100, 1) + '%' : 'n/a'}</div>
					<div>Status: {hoverInfo.cluster.status}</div>
				</div>
			{/if}
		</div>

		<div class="legend">
			<div><span class="swatch" style="background:#16a34a"></span> Solved</div>
			<div><span class="swatch" style="background:#dc2626"></span> Timeout</div>
			<div class="note">Size = iterations</div>
		</div>
	{:else}
		<div class="empty-state">No cluster data available.</div>
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

	.hover-overlay {
		fill: transparent;
		pointer-events: all;
	}

	.hover-ring {
		fill: none;
		stroke: #c44b23;
		stroke-width: 2.5;
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
		width: 0.7rem;
		height: 0.7rem;
		border-radius: 50%;
		display: inline-block;
	}

	.note {
		font-style: italic;
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
