<script lang="ts">
	import type { ClusterCentroid } from '$lib/clustering-browser';
	import type { FeatureCollection, MapDemandRow } from '$lib/types';

	type MapSummary = {
		warehouse?: { latitude?: number; longitude?: number } | null;
		available_demand_dates?: string[];
	};

	type MapMode = 'customers' | 'total' | 'scenario' | 'clusters';
	type SliceBoundary = {
		angle: number;
		clusterBefore: number;
		clusterAfter: number;
	};

	let {
		data = null,
		summary = null,
		demandRows = [],
		allowedModes = ['customers', 'total', 'scenario', 'clusters'],
		defaultMode = undefined,
		selectionRadiusKm = null,
		includedClientNums = null,
		onCustomerClick = null,
		clusterAssignments = null,
		clusterCentroids = [],
		sliceBoundaries = [],
		showClusterCentroids = true,
		height = 520
	} = $props<{
		data?: FeatureCollection | null;
		summary?: Record<string, unknown> | null;
		demandRows?: MapDemandRow[];
		allowedModes?: MapMode[];
		defaultMode?: MapMode;
		selectionRadiusKm?: number | null;
		includedClientNums?: Set<number> | null;
		onCustomerClick?: ((clientNum: number) => void) | null;
		clusterAssignments?: Map<number, number> | null;
		clusterCentroids?: ClusterCentroid[];
		sliceBoundaries?: SliceBoundary[];
		showClusterCentroids?: boolean;
		height?: number;
	}>();

	const width = 960;
	const padding = { top: 44, right: 44, bottom: 48, left: 44 };
	const scenarioDateFallback = 'No scenario';
	const clusterPalette = [
		'#1f77b4',
		'#d62728',
		'#2ca02c',
		'#ff7f0e',
		'#9467bd',
		'#17becf',
		'#8c564b',
		'#e377c2',
		'#bcbd22',
		'#7f7f7f',
		'#003f5c',
		'#ef5675',
		'#2f4b7c',
		'#ffa600',
		'#00a676',
		'#c1121f',
		'#118ab2',
		'#6a4c93',
		'#f9844a',
		'#4d908e'
	] as const;

	let mode = $state<MapMode>('customers');
	let modeInitialized = $state(false);
	let selectedScenarioIndex = $state(0);
	let hoveredClientNum = $state<number | null>(null);

	const enabledModes = $derived.by(() => {
		const set = new Set(allowedModes);
		if (!hasClusterAssignments) set.delete('clusters');
		return (['customers', 'total', 'scenario', 'clusters'] as MapMode[]).filter((value) => set.has(value));
	});
	const showModeTabs = $derived(enabledModes.length > 1);

	const typedSummary = $derived((summary as MapSummary | null) ?? null);
	const features = $derived(data?.features ?? []);
	const warehouse = $derived.by(() => {
		const latitude = Number(typedSummary?.warehouse?.latitude);
		const longitude = Number(typedSummary?.warehouse?.longitude);
		if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
		return { latitude, longitude };
	});

	const bounds = $derived.by(() => {
		const xs = features.map((feature: FeatureCollection['features'][number]) => feature.geometry.coordinates[0]);
		const ys = features.map((feature: FeatureCollection['features'][number]) => feature.geometry.coordinates[1]);
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

	const scenarioDates = $derived.by((): string[] => {
		const datesFromSummary = Array.isArray(typedSummary?.available_demand_dates)
			? typedSummary.available_demand_dates.filter((value): value is string => typeof value === 'string' && value.length > 0)
			: [];
		if (datesFromSummary.length) return datesFromSummary;
		const datesFromRows: string[] = [];
		for (const row of demandRows) {
			if (typeof row.delivery_date === 'string' && row.delivery_date.length > 0) {
				datesFromRows.push(row.delivery_date);
			}
		}
		return Array.from(new Set(datesFromRows)).sort();
	});
	const showScenarioControls = $derived(
		mode === 'scenario' && enabledModes.includes('scenario') && scenarioDates.length > 0
	);
	const hasMapHeader = $derived(showModeTabs || showScenarioControls);

	const demandLookup = $derived.by(() => {
		const lookup = new Map<string, Map<number, number>>();
		for (const row of demandRows) {
			const key = row.delivery_date;
			if (!key) continue;
			const byClient = lookup.get(key) ?? new Map<number, number>();
			byClient.set(row.client_num, row.demand_kg);
			lookup.set(key, byClient);
		}
		return lookup;
	});

	$effect(() => {
		if (!scenarioDates.length) {
			selectedScenarioIndex = 0;
			return;
		}
		if (selectedScenarioIndex < 0) {
			selectedScenarioIndex = 0;
		} else if (selectedScenarioIndex >= scenarioDates.length) {
			selectedScenarioIndex = scenarioDates.length - 1;
		}
	});

	const selectedScenarioDate = $derived.by((): string | null => scenarioDates[selectedScenarioIndex] ?? null);
	const selectedScenarioDemand = $derived.by(() => {
		const date = selectedScenarioDate;
		if (!date) return new Map<number, number>();
		return demandLookup.get(date) ?? new Map<number, number>();
	});

	const hoveredFeature = $derived.by(() => {
		if (hoveredClientNum === null) return null;
		return features.find((feature: FeatureCollection['features'][number]) => Number(feature.properties.client_num) === hoveredClientNum) ?? null;
	});
	const hasClusterAssignments = $derived.by(() => {
		if ((clusterAssignments?.size ?? 0) > 0) return true;
		return features.some((feature: FeatureCollection['features'][number]) => {
			return parsedClusterId(feature.properties.cluster_id) !== null;
		});
	});

	const totalDemandMax = $derived.by(() => {
		let max = 0;
		for (const feature of features) {
			max = Math.max(max, numeric(feature.properties.total_demand));
		}
		return max;
	});

	const scenarioDemandMax = $derived.by(() => {
		let max = 0;
		for (const value of selectedScenarioDemand.values()) {
			max = Math.max(max, value);
		}
		return max;
	});

	function numeric(value: unknown): number {
		const number = Number(value);
		return Number.isFinite(number) ? number : 0;
	}

	function parsedClusterId(value: unknown): number | null {
		if (value === null || value === undefined || value === '') return null;
		const clusterId = Number(value);
		return Number.isFinite(clusterId) ? clusterId : null;
	}

	function clientNumOf(feature: FeatureCollection['features'][number]): number {
		return Number(feature.properties.client_num);
	}

	function labelOf(feature: FeatureCollection['features'][number]): string {
		return String(feature.properties.store_name ?? feature.properties.customer_name ?? feature.properties.store_id ?? feature.properties.customer_id ?? feature.properties.client_num ?? 'Store');
	}

	function formatDemand(value: number): string {
		if (!Number.isFinite(value)) return 'n/a';
		return new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(value);
	}

	function xFor(value: number): number {
		if (!bounds) return 0;
		const span = bounds.maxX - bounds.minX || 1;
		return padding.left + ((value - bounds.minX) / span) * (width - padding.left - padding.right);
	}

	function yFor(value: number): number {
		if (!bounds) return 0;
		const span = bounds.maxY - bounds.minY || 1;
		return height - padding.bottom - ((value - bounds.minY) / span) * (height - padding.top - padding.bottom);
	}

	function scaledRadius(value: number, maxValue: number, minimum: number, maximum: number): number {
		if (value <= 0 || maxValue <= 0) return minimum;
		return minimum + (Math.sqrt(value) / Math.sqrt(maxValue)) * (maximum - minimum);
	}

	function radiusFor(feature: FeatureCollection['features'][number]): number {
		if (mode === 'customers') return 2.5;
		if (mode === 'clusters') return 3;
		if (mode === 'total') return scaledRadius(numeric(feature.properties.total_demand), totalDemandMax, 1.8, 7);
		return scaledRadius(selectedScenarioDemand.get(clientNumOf(feature)) ?? 0, scenarioDemandMax, 1.6, 6.5);
	}

	function clusterIdOf(feature: FeatureCollection['features'][number]): number | null {
		const clientNum = clientNumOf(feature);
		if (clusterAssignments?.has(clientNum)) return clusterAssignments.get(clientNum) ?? null;
		return parsedClusterId(feature.properties.cluster_id);
	}

	function clusterColor(clusterId: number | null): string {
		if (clusterId === null || clusterId < 0) return '#b7b7b7';
		return clusterPalette[clusterId % clusterPalette.length] ?? '#b7b7b7';
	}

	function fillFor(feature: FeatureCollection['features'][number]): string {
		if (mode === 'customers') return '#1f2937';
		if (mode === 'clusters') return clusterColor(clusterIdOf(feature));
		if (mode === 'total') return '#2563eb';
		return (selectedScenarioDemand.get(clientNumOf(feature)) ?? 0) > 0 ? '#c2410c' : '#b7b7b7';
	}

	function isIncluded(feature: FeatureCollection['features'][number]): boolean {
		if (!includedClientNums) return true;
		return includedClientNums.has(clientNumOf(feature));
	}

	function opacityFor(feature: FeatureCollection['features'][number]): number {
		const included = isIncluded(feature);
		if (mode !== 'scenario') return included ? 0.78 : 0.16;
		if (!included) return 0.16;
		return (selectedScenarioDemand.get(clientNumOf(feature)) ?? 0) > 0 ? 0.84 : 0.55;
	}

	function strokeFor(feature: FeatureCollection['features'][number]): string {
		if (!isIncluded(feature)) return '#d1d5db';
		if (mode === 'clusters') return '#ffffff';
		if (mode === 'scenario' && (selectedScenarioDemand.get(clientNumOf(feature)) ?? 0) <= 0) return '#9a9a9a';
		return '#ffffff';
	}

	function strokeWidthFor(feature: FeatureCollection['features'][number]): number {
		if (!isIncluded(feature)) return 0.95;
		return mode === 'clusters' ? 1.2 : 1.1;
	}

	function handleCustomerClick(feature: FeatureCollection['features'][number]): void {
		onCustomerClick?.(clientNumOf(feature));
	}

	$effect(() => {
		const fallbackMode = enabledModes[0] ?? 'total';
		if (!modeInitialized) {
			modeInitialized = true;
			if (defaultMode && enabledModes.includes(defaultMode)) {
				mode = defaultMode;
				return;
			}
			if (hasClusterAssignments && enabledModes.includes('clusters')) {
				mode = 'clusters';
				return;
			}
			mode = fallbackMode;
			return;
		}
		if (!enabledModes.includes(mode)) {
			mode = defaultMode && enabledModes.includes(defaultMode) ? defaultMode : fallbackMode;
		}
	});

	function radiusOverlay(): { cx: number; cy: number; rx: number; ry: number } | null {
		if (!warehouse || !selectionRadiusKm || selectionRadiusKm <= 0) return null;
		const latDelta = selectionRadiusKm / 110.574;
		const cosLat = Math.cos((warehouse.latitude * Math.PI) / 180);
		const lonDelta = selectionRadiusKm / (111.32 * Math.max(0.2, Math.abs(cosLat)));
		return {
			cx: xFor(warehouse.longitude),
			cy: yFor(warehouse.latitude),
			rx: Math.abs(xFor(warehouse.longitude + lonDelta) - xFor(warehouse.longitude)),
			ry: Math.abs(yFor(warehouse.latitude) - yFor(warehouse.latitude + latDelta))
		};
	}

	function shiftScenario(step: number): void {
		if (!scenarioDates.length) return;
		selectedScenarioIndex = Math.max(0, Math.min(scenarioDates.length - 1, selectedScenarioIndex + step));
	}

	function handleScenarioWheel(event: WheelEvent): void {
		if (mode !== 'scenario' || !scenarioDates.length) return;
		event.preventDefault();
		shiftScenario(event.deltaY > 0 ? 1 : -1);
	}

	function sliceBoundaryLine(angle: number): { x1: number; y1: number; x2: number; y2: number } | null {
		if (!warehouse || !Number.isFinite(angle)) return null;
		const x1 = xFor(warehouse.longitude);
		const y1 = yFor(warehouse.latitude);
		const reach = Math.hypot(width, height);
		return {
			x1,
			y1,
			x2: x1 + Math.cos(angle) * reach,
			y2: y1 - Math.sin(angle) * reach
		};
	}

	function normalizedAngle(angle: number): number {
		let value = angle;
		while (value <= -Math.PI) value += 2 * Math.PI;
		while (value > Math.PI) value -= 2 * Math.PI;
		return value;
	}

	function angleDelta(start: number, end: number): number {
		let delta = end - start;
		if (delta < 0) delta += 2 * Math.PI;
		return delta;
	}

	function sectorPoint(angle: number, reach: number): { x: number; y: number } | null {
		if (!warehouse || !Number.isFinite(angle)) return null;
		const cx = xFor(warehouse.longitude);
		const cy = yFor(warehouse.latitude);
		return {
			x: cx + Math.cos(angle) * reach,
			y: cy - Math.sin(angle) * reach
		};
	}

	function sectorPath(startAngle: number, endAngle: number): string | null {
		if (!warehouse) return null;
		const cx = xFor(warehouse.longitude);
		const cy = yFor(warehouse.latitude);
		const reach = Math.hypot(width, height) * 1.25;
		const start = sectorPoint(startAngle, reach);
		const end = sectorPoint(endAngle, reach);
		if (!start || !end) return null;

		const delta = angleDelta(startAngle, endAngle);
		const steps = Math.max(2, Math.ceil((delta / (2 * Math.PI)) * 64));
		const path = [`M ${cx} ${cy}`, `L ${start.x} ${start.y}`];
		for (let step = 1; step < steps; step += 1) {
			const angle = normalizedAngle(startAngle + (delta * step) / steps);
			const point = sectorPoint(angle, reach);
			if (!point) continue;
			path.push(`L ${point.x} ${point.y}`);
		}
		path.push(`L ${end.x} ${end.y}`, 'Z');
		return path.join(' ');
	}

	const clusterSectors = $derived.by(() => {
		if (!warehouse || sliceBoundaries.length < 2) return [];
		const sorted: SliceBoundary[] = sliceBoundaries
			.filter(
				(boundary: SliceBoundary): boundary is SliceBoundary =>
					Number.isFinite(boundary.angle) &&
					Number.isFinite(boundary.clusterBefore) &&
					Number.isFinite(boundary.clusterAfter)
			)
			.slice()
			.sort((a: SliceBoundary, b: SliceBoundary) => a.angle - b.angle);
		if (sorted.length < 2) return [];

		return sorted
			.map((boundary: SliceBoundary, index: number) => {
				const next = sorted[(index + 1) % sorted.length];
				const path = sectorPath(boundary.angle, next.angle);
				if (!path) return null;
				return {
					clusterId: boundary.clusterAfter,
					path
				};
			})
			.filter(
				(sector: { clusterId: number; path: string } | null): sector is { clusterId: number; path: string } =>
					sector !== null
			);
	});
</script>

{#if !features.length}
	<div class="empty">No map data available.</div>
{:else}
	<section class="map-shell">
		{#if hasMapHeader}
			<div class="map-header">
				<div class="map-toolbar">
					{#if showModeTabs}
						<div class="tablist" role="tablist" aria-label="Map views">
							{#if enabledModes.includes('customers')}
								<button class:active={mode === 'customers'} type="button" onclick={() => (mode = 'customers')}>Stores</button>
							{/if}
							{#if enabledModes.includes('total')}
								<button class:active={mode === 'total'} type="button" onclick={() => (mode = 'total')}>Total Demand</button>
							{/if}
							{#if enabledModes.includes('scenario')}
								<button class:active={mode === 'scenario'} type="button" onclick={() => (mode = 'scenario')}>Demand By Scenario</button>
							{/if}
							{#if enabledModes.includes('clusters')}
								<button class:active={mode === 'clusters'} type="button" onclick={() => (mode = 'clusters')}>Clusters</button>
							{/if}
						</div>
					{/if}

					<div
						class="scenario-controls"
						class:is-hidden={!showScenarioControls}
						aria-hidden={!showScenarioControls}
						onwheel={handleScenarioWheel}
					>
						<button type="button" onclick={() => shiftScenario(-1)} disabled={mode !== 'scenario' || selectedScenarioIndex <= 0}>Prev</button>
						<input
							type="range"
							min="0"
							max={Math.max(0, scenarioDates.length - 1)}
							step="1"
							bind:value={selectedScenarioIndex}
							disabled={mode !== 'scenario' || !scenarioDates.length}
							aria-label="Scenario slider"
						/>
						<select
							value={selectedScenarioDate ?? ''}
							onchange={(event) => {
								const next = (event.currentTarget as HTMLSelectElement).value;
								const index = scenarioDates.indexOf(next);
								if (index >= 0) selectedScenarioIndex = index;
							}}
							disabled={mode !== 'scenario' || !scenarioDates.length}
						>
							{#if !scenarioDates.length}
								<option>{scenarioDateFallback}</option>
							{:else}
								{#each scenarioDates as date}
									<option value={date}>{date}</option>
								{/each}
							{/if}
						</select>
						<button
							type="button"
							onclick={() => shiftScenario(1)}
							disabled={mode !== 'scenario' || selectedScenarioIndex >= scenarioDates.length - 1}
						>
							Next
						</button>
					</div>
				</div>
			</div>
		{/if}

		<div class="map-frame">
			<svg viewBox={`0 0 ${width} ${height}`} aria-label="Store map">
				<line
					x1={padding.left}
					y1={height - padding.bottom}
					x2={width - padding.right}
					y2={height - padding.bottom}
					class="axis"
				/>
				<line
					x1={padding.left}
					y1={padding.top}
					x2={padding.left}
					y2={height - padding.bottom}
					class="axis"
				/>

				{#if radiusOverlay()}
					{@const overlay = radiusOverlay()}
					{#if overlay}
						<ellipse
							cx={overlay.cx}
							cy={overlay.cy}
							rx={overlay.rx}
							ry={overlay.ry}
							class="radius-overlay"
						/>
					{/if}
				{/if}

				{#if mode === 'clusters' && warehouse && clusterSectors.length}
					{#each clusterSectors as sector}
						<path d={sector.path} fill={clusterColor(sector.clusterId)} fill-opacity="0.12" stroke="none" class="slice-sector">
							<title>Cluster {sector.clusterId} sector</title>
						</path>
					{/each}
				{/if}

				{#if mode === 'clusters' && warehouse && sliceBoundaries.length}
					{#each sliceBoundaries as boundary, index}
						{@const boundaryLine = sliceBoundaryLine(boundary.angle)}
						{#if boundaryLine}
							<line
								x1={boundaryLine.x1}
								y1={boundaryLine.y1}
								x2={boundaryLine.x2}
								y2={boundaryLine.y2}
								class="slice-boundary"
							>
								<title>Boundary {index + 1}: cluster {boundary.clusterBefore} to {boundary.clusterAfter}</title>
							</line>
						{/if}
					{/each}
				{/if}

				{#if mode === 'clusters' && showClusterCentroids}
					{#each clusterCentroids as centroid}
						<g class="centroid" transform={`translate(${xFor(centroid.longitude)} ${yFor(centroid.latitude)})`}>
							<rect
								x="-5"
								y="-5"
								width="10"
								height="10"
								fill={clusterColor(centroid.cluster_id)}
								stroke="#111827"
								stroke-width="1"
								vector-effect="non-scaling-stroke"
								transform="rotate(45)"
							/>
							<title>Cluster {centroid.cluster_id} centroid ({centroid.size} stores)</title>
						</g>
					{/each}
				{/if}

				{#each features as feature}
					{@const lon = feature.geometry.coordinates[0]}
					{@const lat = feature.geometry.coordinates[1]}
					{@const totalDemand = numeric(feature.properties.total_demand)}
					{@const scenarioDemand = selectedScenarioDemand.get(clientNumOf(feature)) ?? 0}
					<!-- svelte-ignore a11y_no_noninteractive_element_interactions, a11y_click_events_have_key_events -->
					<circle
						cx={xFor(lon)}
						cy={yFor(lat)}
						r={radiusFor(feature)}
						fill={fillFor(feature)}
						fill-opacity={opacityFor(feature)}
						stroke={strokeFor(feature)}
						stroke-width={strokeWidthFor(feature)}
						vector-effect="non-scaling-stroke"
						role="img"
						aria-label={labelOf(feature)}
						class:hovered={hoveredClientNum === clientNumOf(feature)}
						class:selectable={Boolean(onCustomerClick)}
						class:excluded={!isIncluded(feature)}
						onmouseenter={() => (hoveredClientNum = clientNumOf(feature))}
						onmouseleave={() => (hoveredClientNum = null)}
						onclick={() => handleCustomerClick(feature)}
					>
						<title>
							{labelOf(feature)}
							{mode === 'clusters' ? `\nCluster: ${clusterIdOf(feature) ?? 'n/a'}` : ''}
							{`\nTotal demand: ${formatDemand(totalDemand)} kg`}
							{mode === 'scenario' && selectedScenarioDate ? `\n${selectedScenarioDate}: ${formatDemand(scenarioDemand)} kg` : ''}
						</title>
					</circle>
				{/each}

				{#if warehouse}
					<g class="warehouse">
						<rect
							x={xFor(warehouse.longitude) - 7}
							y={yFor(warehouse.latitude) - 7}
							width="14"
							height="14"
							class="warehouse-backdrop"
						/>
						<rect
							x={xFor(warehouse.longitude) - 5.5}
							y={yFor(warehouse.latitude) - 5.5}
							width="11"
							height="11"
						/>
						<title>Warehouse</title>
					</g>
				{/if}
			</svg>
		</div>

		<div class="map-footer">
			<div class="legend">
				<span class="legend-item"><span class="swatch points"></span> store</span>
				{#if mode === 'clusters'}
					<span class="legend-item"><span class="swatch cluster"></span> cluster assignment</span>
					{#if clusterSectors.length}
						<span class="legend-item"><span class="swatch slice-sector"></span> angular sector</span>
					{/if}
					{#if sliceBoundaries.length}
						<span class="legend-item"><span class="swatch slice-line"></span> slice boundary</span>
					{/if}
					{#if showClusterCentroids}
						<span class="legend-item"><span class="swatch centroid-mark"></span> centroid</span>
					{/if}
				{:else if mode === 'total'}
					<span class="legend-item"><span class="swatch total"></span> larger = higher total demand</span>
				{:else if mode === 'scenario'}
					<span class="legend-item"><span class="swatch scenario"></span> active demand</span>
					<span class="legend-item"><span class="swatch zero"></span> zero demand</span>
				{/if}
				{#if warehouse}
					<span class="legend-item"><span class="swatch warehouse-mark">+</span> warehouse</span>
				{/if}
				{#if selectionRadiusKm}
					<span class="legend-item"><span class="swatch radius-ring"></span> selected radius</span>
				{/if}
				{#if includedClientNums}
					<span class="legend-item"><span class="swatch excluded-point"></span> excluded store</span>
				{/if}
			</div>

			{#if hoveredFeature}
				<div class="hover-card">
					<strong>{labelOf(hoveredFeature)}</strong>
					<span>Client {clientNumOf(hoveredFeature)}</span>
					{#if hasClusterAssignments}
						<span>Cluster {clusterIdOf(hoveredFeature) ?? 'n/a'}</span>
					{/if}
					<span>Total {formatDemand(numeric(hoveredFeature.properties.total_demand))} kg</span>
					{#if mode === 'scenario' && selectedScenarioDate}
						<span>{selectedScenarioDate}: {formatDemand(selectedScenarioDemand.get(clientNumOf(hoveredFeature)) ?? 0)} kg</span>
					{/if}
				</div>
			{/if}
		</div>
	</section>
{/if}

<style>
	.map-shell {
		display: grid;
		gap: 0.85rem;
	}

	.map-header {
		min-height: var(--map-header-offset, 5.5rem);
		display: grid;
		align-content: start;
		gap: 0.85rem;
	}

	.map-toolbar {
		display: flex;
		flex-wrap: wrap;
		justify-content: space-between;
		gap: 0.75rem;
		align-items: center;
	}

	.tablist {
		display: flex;
		flex-wrap: wrap;
		gap: 0.4rem;
	}

	.tablist button,
	.scenario-controls button {
		border: 1px solid var(--border);
		background: transparent;
		color: var(--text);
	}

	.tablist button.active {
		border-color: #7aa2e3;
		color: #184a97;
	}

	.scenario-controls {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 0.5rem;
	}

	.scenario-controls.is-hidden {
		visibility: hidden;
		pointer-events: none;
	}

	.scenario-controls input[type='range'] {
		width: min(18rem, 48vw);
		padding: 0;
	}

	.map-frame {
		border: 1px solid var(--border);
		padding: 0.25rem;
	}

	svg {
		display: block;
		width: 100%;
		height: auto;
	}

	.axis {
		stroke: #d7d7d7;
		stroke-width: 1;
	}

	.radius-overlay {
		fill: rgba(37, 99, 235, 0.08);
		stroke: #2563eb;
		stroke-width: 1.2;
		stroke-dasharray: 6 4;
		vector-effect: non-scaling-stroke;
	}

	.slice-boundary {
		stroke: rgba(17, 24, 39, 0.6);
		stroke-width: 1.35;
		stroke-dasharray: 8 6;
		vector-effect: non-scaling-stroke;
	}

	.slice-sector {
		pointer-events: none;
	}

	circle {
		transition:
			r 180ms ease,
			fill 220ms ease,
			fill-opacity 180ms ease,
			stroke 180ms ease,
			stroke-width 180ms ease;
	}

	circle.hovered {
		stroke: #111;
		stroke-width: 1.5;
	}

	circle.selectable {
		cursor: pointer;
	}

	circle.excluded {
		stroke-dasharray: 2.5 2.5;
	}

	.warehouse rect {
		fill: rgba(37, 99, 235, 0.88);
		stroke: #1e40af;
		stroke-width: 1.5;
		vector-effect: non-scaling-stroke;
		transition: fill 180ms ease, stroke 180ms ease, opacity 180ms ease;
	}

	.warehouse-backdrop {
		fill: rgba(255, 255, 255, 0.82);
		stroke: #0f172a;
		stroke-width: 1.25;
		vector-effect: non-scaling-stroke;
	}

	.map-footer {
		display: flex;
		flex-wrap: wrap;
		justify-content: space-between;
		gap: 0.75rem;
		align-items: flex-start;
	}

	.legend {
		display: flex;
		flex-wrap: wrap;
		gap: 0.75rem;
	}

	.legend-item {
		display: inline-flex;
		align-items: center;
		gap: 0.35rem;
		font-size: 0.86rem;
		color: var(--muted);
	}

	.swatch {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		width: 0.9rem;
		height: 0.9rem;
		border: 1px solid transparent;
	}

	.swatch.points {
		background: #1f2937;
		border-radius: 0;
	}

	.swatch.total {
		background: #2563eb;
		border-radius: 0;
	}

	.swatch.cluster {
		background: linear-gradient(135deg, #d97706, #2563eb);
		border-radius: 0;
	}

	.swatch.slice-sector {
		background: rgba(37, 99, 235, 0.18);
		border-color: rgba(37, 99, 235, 0.35);
		border-radius: 0;
	}

	.swatch.slice-line {
		width: 1.2rem;
		height: 0;
		border-top: 2px dashed rgba(17, 24, 39, 0.75);
		border-radius: 0;
		background: transparent;
	}

	.swatch.centroid-mark {
		width: 0.8rem;
		height: 0.8rem;
		background: #111827;
		transform: rotate(45deg);
	}

	.swatch.scenario {
		background: #c2410c;
		border-radius: 0;
	}

	.swatch.zero {
		background: #b7b7b7;
		border-radius: 0;
	}

	.swatch.warehouse-mark {
		width: 0.75rem;
		height: 0.75rem;
		background: #2563eb;
		border: 1px solid #1e40af;
	}

	.swatch.radius-ring {
		border: 1px dashed #2563eb;
		background: rgba(37, 99, 235, 0.1);
	}

	.swatch.excluded-point {
		background: #f3f4f6;
		border-color: #9ca3af;
		border-style: dashed;
		border-radius: 0;
	}

	.hover-card {
		display: grid;
		gap: 0.2rem;
		font-size: 0.86rem;
		border: 1px solid var(--border);
		padding: 0.6rem 0.75rem;
		min-width: 12rem;
	}

	.empty {
		border: 1px dashed var(--border);
		padding: 2rem;
		text-align: center;
		color: var(--muted);
	}

	@media (max-width: 760px) {
		.map-toolbar,
		.map-footer {
			display: grid;
		}

		.scenario-controls {
			grid-template-columns: 1fr;
			display: grid;
		}

		.scenario-controls input[type='range'],
		.scenario-controls select {
			width: 100%;
		}
	}
</style>
