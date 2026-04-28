<script lang="ts">
	import { formatNumber } from '$lib/format';
	import type { FeatureCollection } from '$lib/types';

	type MapSummary = {
		warehouse?: { latitude?: number; longitude?: number } | null;
	};

	type RouteSegment = {
		from?: number | null;
		to?: number | null;
		truck?: number | null;
		tour?: number | null;
		from_latitude?: number | null;
		from_longitude?: number | null;
		to_latitude?: number | null;
		to_longitude?: number | null;
	};

	type ChargerMarker = {
		customer?: number | null;
		customer_name?: string | null;
		customer_id?: string | null;
		cluster_id?: number | null;
		latitude?: number | null;
		longitude?: number | null;
		energy_kwh?: number | null;
		status?: string | null;
		tau?: number | null;
	};

	type ChargerTypeInfo = { id: number; name: string; power_kw: number; color: string };

	const CHARGER_TYPES: ChargerTypeInfo[] = [
		{ id: 1, name: '22 kW AC', power_kw: 22, color: '#22c55e' },
		{ id: 2, name: '43 kW AC', power_kw: 43, color: '#a3e635' },
		{ id: 3, name: '40 kW DC', power_kw: 40, color: '#06b6d4' },
		{ id: 4, name: '50 kW DC', power_kw: 50, color: '#3b82f6' },
		{ id: 5, name: '90 kW DC', power_kw: 90, color: '#8b5cf6' },
		{ id: 6, name: '120 kW DC', power_kw: 120, color: '#ec4899' },
		{ id: 7, name: '150 kW DC', power_kw: 150, color: '#f97316' },
		{ id: 8, name: '250 kW DC', power_kw: 250, color: '#ef4444' },
	];

	const chargerTypeMap = new Map(CHARGER_TYPES.map((t) => [t.id, t]));

	type HoverCharger = { marker: ChargerMarker; sx: number; sy: number } | null;

	let {
		title,
		subtitle = '',
		data = null,
		summary = null,
		bundle = null,
		height = 480
	} = $props<{
		title: string;
		subtitle?: string;
		data?: FeatureCollection | null;
		summary?: Record<string, unknown> | null;
		bundle?: Record<string, unknown> | null;
		height?: number;
	}>();

	const width = 960;
	const padding = { top: 28, right: 28, bottom: 28, left: 28 };
	const routePalette = ['#1d4ed8', '#16a34a', '#dc2626', '#ea580c', '#7c3aed', '#0891b2', '#be123c', '#4f46e5'];

	function numeric(value: unknown): number | null {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? parsed : null;
	}

	const typedSummary = $derived((summary as MapSummary | null) ?? null);
	const features = $derived(data?.features ?? []);
	const routes = $derived(((bundle?.routes as RouteSegment[] | undefined) ?? []).filter(Boolean));
	const customerChargers = $derived(((bundle?.customer_chargers as ChargerMarker[] | undefined) ?? []).filter(Boolean));
	const warehouse = $derived.by(() => {
		const latitude = numeric(typedSummary?.warehouse?.latitude);
		const longitude = numeric(typedSummary?.warehouse?.longitude);
		return latitude !== null && longitude !== null ? { latitude, longitude } : null;
	});

	const bounds = $derived.by(() => {
		const xs: number[] = [];
		const ys: number[] = [];
		for (const feature of features) {
			xs.push(feature.geometry.coordinates[0]);
			ys.push(feature.geometry.coordinates[1]);
		}
		for (const route of routes) {
			const fromLongitude = numeric(route.from_longitude);
			const toLongitude = numeric(route.to_longitude);
			const fromLatitude = numeric(route.from_latitude);
			const toLatitude = numeric(route.to_latitude);
			if (fromLongitude !== null) xs.push(fromLongitude);
			if (toLongitude !== null) xs.push(toLongitude);
			if (fromLatitude !== null) ys.push(fromLatitude);
			if (toLatitude !== null) ys.push(toLatitude);
		}
		for (const marker of customerChargers) {
			const longitude = numeric(marker.longitude);
			const latitude = numeric(marker.latitude);
			if (longitude !== null) xs.push(longitude);
			if (latitude !== null) ys.push(latitude);
		}
		if (warehouse) {
			xs.push(warehouse.longitude);
			ys.push(warehouse.latitude);
		}
		if (!xs.length || !ys.length) return null;
		return {
			minX: Math.min(...xs),
			maxX: Math.max(...xs),
			minY: Math.min(...ys),
			maxY: Math.max(...ys)
		};
	});

	function xFor(longitude: number): number {
		if (!bounds) return 0;
		const span = bounds.maxX - bounds.minX || 1;
		return padding.left + ((longitude - bounds.minX) / span) * (width - padding.left - padding.right);
	}

	function yFor(latitude: number): number {
		if (!bounds) return 0;
		const span = bounds.maxY - bounds.minY || 1;
		return height - padding.bottom - ((latitude - bounds.minY) / span) * (height - padding.top - padding.bottom);
	}

	function routeColor(route: RouteSegment): string {
		const truck = numeric(route.truck) ?? 0;
		const tour = numeric(route.tour) ?? 0;
		return routePalette[Math.abs(truck * 7 + tour) % routePalette.length] ?? '#1d4ed8';
	}

	let hoverCharger = $state<HoverCharger>(null);

	const hasDeltaStatus = $derived(customerChargers.some((m) => {
		const s = String(m.status ?? '').toLowerCase();
		return s === 'added' || s === 'removed' || s === 'unchanged';
	}));

	function chargerColor(marker: ChargerMarker): string {
		const status = String(marker.status ?? 'installed').toLowerCase();
		if (status === 'removed') return '#dc2626';
		const tau = numeric(marker.tau);
		if (tau !== null) {
			const info = chargerTypeMap.get(tau);
			if (info) return info.color;
		}
		if (status === 'added') return '#16a34a';
		if (status === 'unchanged') return '#1d4ed8';
		return '#15803d';
	}

	function chargerRadius(marker: ChargerMarker): number {
		const status = String(marker.status ?? '').toLowerCase();
		if (status === 'removed') return 3;
		return 4;
	}

	function chargerOpacity(marker: ChargerMarker): number {
		const status = String(marker.status ?? '').toLowerCase();
		if (status === 'removed') return 0.5;
		return 0.9;
	}

	function chargerTypeLabel(marker: ChargerMarker): string {
		const tau = numeric(marker.tau);
		if (tau !== null) {
			const info = chargerTypeMap.get(tau);
			if (info) return info.name;
		}
		return 'Unknown';
	}

	function handleChargerHover(event: PointerEvent): void {
		const target = event.currentTarget;
		if (!(target instanceof SVGRectElement)) return;
		const rect = target.getBoundingClientRect();
		const sx = ((event.clientX - rect.left) / rect.width) * width;
		const sy = ((event.clientY - rect.top) / rect.height) * height;
		let nearest: HoverCharger = null;
		let nearestDist = 225;
		for (const marker of customerChargers) {
			const lon = numeric(marker.longitude);
			const lat = numeric(marker.latitude);
			if (lon === null || lat === null) continue;
			const cx = xFor(lon);
			const cy = yFor(lat);
			const d = (cx - sx) ** 2 + (cy - sy) ** 2;
			if (d < nearestDist) {
				nearestDist = d;
				nearest = { marker, sx: cx, sy: cy };
			}
		}
		hoverCharger = nearest;
	}

	const routeCount = $derived(routes.length);
	const chargerCount = $derived(customerChargers.length);
	const activeChargerTypes = $derived.by(() => {
		const taus = new Set(customerChargers.map((m) => numeric(m.tau)).filter((t): t is number => t !== null));
		return CHARGER_TYPES.filter((t) => taus.has(t.id));
	});
	const summaryItems = $derived.by(() => {
		const values = bundle?.summary as Record<string, unknown> | undefined;
		if (!values) return [];
		return [
			values.total_cost !== undefined ? `Cost ${formatNumber(values.total_cost, 2)}` : null,
			values.route_count !== undefined ? `${formatNumber(values.route_count, 0)} routes` : null,
			values.truck_count !== undefined ? `${formatNumber(values.truck_count, 0)} trucks` : null,
			values.customer_charge_total_kwh !== undefined ? `${formatNumber(values.customer_charge_total_kwh, 1)} kWh stores` : null,
			values.added_customer_chargers !== undefined ? `+${formatNumber(values.added_customer_chargers, 0)} added` : null,
			values.removed_customer_chargers !== undefined ? `-${formatNumber(values.removed_customer_chargers, 0)} removed` : null,
			values.installed_customer_chargers !== undefined ? `${formatNumber(values.installed_customer_chargers, 0)} chargers` : null
		].filter(Boolean);
	});
</script>

<section class="map-card">
	<div class="card-header">
		<div>
			<h3>{title}</h3>
			{#if subtitle}
				<p>{subtitle}</p>
			{/if}
		</div>
		{#if summaryItems.length}
			<div class="summary-strip">{summaryItems.join(' · ')}</div>
		{/if}
	</div>

	{#if bounds}
		<div class="map-frame">
			<svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={title}>
				<rect x="0" y="0" width={width} height={height} fill="#f8fafc" />

				{#each features as feature}
					<circle
						cx={xFor(feature.geometry.coordinates[0])}
						cy={yFor(feature.geometry.coordinates[1])}
						r="2"
						fill="#cbd5e1"
						opacity="0.8"
					/>
				{/each}

				{#each routes as route}
					{#if numeric(route.from_longitude) !== null && numeric(route.from_latitude) !== null && numeric(route.to_longitude) !== null && numeric(route.to_latitude) !== null}
						<line
							x1={xFor(numeric(route.from_longitude)!)}
							y1={yFor(numeric(route.from_latitude)!)}
							x2={xFor(numeric(route.to_longitude)!)}
							y2={yFor(numeric(route.to_latitude)!)}
							stroke={routeColor(route)}
							stroke-width="2.2"
							stroke-linecap="round"
							opacity="0.72"
						/>
					{/if}
				{/each}

				{#each customerChargers as marker}
					{#if numeric(marker.longitude) !== null && numeric(marker.latitude) !== null}
						<circle
							cx={xFor(numeric(marker.longitude)!)}
							cy={yFor(numeric(marker.latitude)!)}
							r={chargerRadius(marker)}
							fill={chargerColor(marker)}
							stroke="#fff"
							stroke-width="1.4"
							opacity={chargerOpacity(marker)}
						/>
					{/if}
				{/each}

				{#if hoverCharger}
					<circle cx={hoverCharger.sx} cy={hoverCharger.sy} r={6} fill="none" stroke="#1e293b" stroke-width="2" />
				{/if}

				{#if warehouse}
					<rect
						x={xFor(warehouse.longitude) - 7}
						y={yFor(warehouse.latitude) - 7}
						width="14"
						height="14"
						fill="#1e3a8a"
						stroke="#fff"
						stroke-width="2"
					/>
				{/if}

				{#if chargerCount}
					<!-- svelte-ignore a11y_no_static_element_interactions -->
					<rect
						x={padding.left} y={padding.top}
						width={width - padding.left - padding.right}
						height={height - padding.top - padding.bottom}
						fill="transparent"
						onpointermove={handleChargerHover}
						onpointerleave={() => (hoverCharger = null)}
					/>
				{/if}
			</svg>

			{#if hoverCharger}
				<div
					class="charger-tooltip"
					style={`left: ${Math.min(Math.max((hoverCharger.sx / width) * 100, 12), 88)}%; top: ${Math.min(Math.max((hoverCharger.sy / height) * 100, 10), 85)}%;`}
				>
					<div><strong>{chargerTypeLabel(hoverCharger.marker)}</strong></div>
					<div>Store {hoverCharger.marker.customer_id ?? hoverCharger.marker.customer ?? 'n/a'}</div>
					<div>Cluster {hoverCharger.marker.cluster_id ?? 'n/a'}</div>
					{#if hoverCharger.marker.energy_kwh != null}
						<div>{formatNumber(hoverCharger.marker.energy_kwh, 1)} kWh</div>
					{/if}
					{#if hoverCharger.marker.status}
						<div>{String(hoverCharger.marker.status)}</div>
					{/if}
				</div>
			{/if}
		</div>

		<div class="legend">
			<div><span class="swatch warehouse"></span> Warehouse</div>
			{#if routeCount}
				<div><span class="swatch route"></span> Routes</div>
			{/if}
			{#each activeChargerTypes as ct}
				<div><span class="swatch" style={`background:${ct.color}`}></span> {ct.name}</div>
			{/each}
			{#if hasDeltaStatus && customerChargers.some((marker) => String(marker.status ?? '').toLowerCase() === 'removed')}
				<div><span class="swatch removed"></span> Removed</div>
			{/if}
		</div>
	{:else}
		<div class="empty-state">No map data available.</div>
	{/if}
</section>

<style>
	.map-card {
		border: 1px solid var(--border);
		background: #fff;
		padding: 0.9rem;
		display: grid;
		gap: 0.8rem;
	}

	.card-header {
		display: grid;
		gap: 0.35rem;
	}

	.card-header h3 {
		margin: 0;
		font-size: 1rem;
	}

	.card-header p {
		margin: 0;
		color: var(--muted);
		font-size: 0.9rem;
	}

	.summary-strip {
		font-size: 0.84rem;
		color: var(--muted);
	}

	.map-frame {
		position: relative;
		border: 1px solid var(--border);
		overflow: hidden;
	}

	.charger-tooltip {
		position: absolute;
		transform: translate(-50%, calc(-100% - 12px));
		pointer-events: none;
		background: rgba(18, 26, 36, 0.94);
		color: #fff;
		padding: 0.5rem 0.65rem;
		font-size: 0.78rem;
		line-height: 1.35;
		white-space: nowrap;
		box-shadow: 0 10px 30px rgba(18, 26, 36, 0.18);
	}

	.legend {
		display: flex;
		flex-wrap: wrap;
		gap: 0.9rem;
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
		height: 0.85rem;
		display: inline-block;
		border-radius: 999px;
		border: 1px solid rgba(15, 23, 42, 0.12);
	}

	.swatch.route {
		background: #1d4ed8;
	}

	.swatch.removed {
		background: #dc2626;
	}

	.swatch.warehouse {
		background: #1e3a8a;
		border-radius: 0.1rem;
	}

	.empty-state {
		border: 1px dashed var(--border);
		padding: 1rem;
		color: var(--muted);
	}
</style>
