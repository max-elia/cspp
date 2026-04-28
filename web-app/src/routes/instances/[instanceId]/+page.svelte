<script lang="ts">
	import { appState } from '$lib/app-state';
	import SimpleMap from '$lib/components/SimpleMap.svelte';
	import { formatDate, formatNumber } from '$lib/format';
	import { page } from '$app/state';
	import { goto } from '$app/navigation';
	import { deleteCatalogItem } from '$lib/run-deletion';
	import type { FeatureCollection, MapDemandRow } from '$lib/types';
	import { onMount } from 'svelte';

	type WarehouseLocation = {
		latitude: number;
		longitude: number;
	};

	type AngularSliceBoundary = {
		angle: number;
		clusterBefore: number;
		clusterAfter: number;
	};

	const instanceId = $derived(page.params.instanceId ?? null);
	let statusMessage = $state('');
	let isDeletingInstance = $state(false);

	onMount(() => {
		appState.selectInstance(instanceId);
		appState.ensureMapData();
	});

	const instance = $derived.by(() =>
		($appState.catalog?.instances ?? []).find((row) => row.instance_id === instanceId) ?? null
	);
	const mapData = $derived($appState.mapData as FeatureCollection | null);
	const mapSummary = $derived($appState.mapSummary as Record<string, unknown> | null);
	const mapDemandRows = $derived($appState.mapDemandRows as MapDemandRow[]);
	const mapFeatures = $derived(mapData?.features ?? []);
	const warehouse = $derived.by((): WarehouseLocation | null => {
		const rawWarehouse = mapSummary?.warehouse;
		if (!rawWarehouse || typeof rawWarehouse !== 'object') return null;
		const latitude = Number((rawWarehouse as Record<string, unknown>).latitude);
		const longitude = Number((rawWarehouse as Record<string, unknown>).longitude);
		if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
		return { latitude, longitude };
	});

	function clusterValue(value: unknown): number | null {
		if (value === null || value === undefined || value === '') return null;
		const number = Number(value);
		return Number.isFinite(number) ? number : null;
	}

	function buildAngularSliceBoundaries(
		features: FeatureCollection['features'],
		center: WarehouseLocation | null
	): AngularSliceBoundary[] {
		if (!center || features.length < 2) return [];
		const sorted = features
			.map((feature) => ({
				clusterId: clusterValue(feature.properties.cluster_id),
				angle: Math.atan2(
					feature.geometry.coordinates[1] - center.latitude,
					feature.geometry.coordinates[0] - center.longitude
				)
			}))
			.filter((entry): entry is { clusterId: number; angle: number } => entry.clusterId !== null)
			.sort((a, b) => a.angle - b.angle);
		if (sorted.length < 2) return [];

		const boundaries: AngularSliceBoundary[] = [];
		for (let index = 0; index < sorted.length; index += 1) {
			const current = sorted[index];
			const next = sorted[(index + 1) % sorted.length];
			if (current.clusterId === next.clusterId) continue;
			let gap = next.angle - current.angle;
			if (gap < 0) gap += 2 * Math.PI;
			let boundaryAngle = current.angle + gap / 2;
			if (boundaryAngle > Math.PI) boundaryAngle -= 2 * Math.PI;
			boundaries.push({
				angle: boundaryAngle,
				clusterBefore: current.clusterId,
				clusterAfter: next.clusterId
			});
		}
		return boundaries;
	}

	const sliceBoundaries = $derived.by(() => buildAngularSliceBoundaries(mapFeatures, warehouse));
	const showCentroids = $derived.by(() => {
		const method = String(instance?.clustering_method ?? '').toLowerCase();
		return method === 'geographic' || method === 'kmeans';
	});

	async function deleteInstance(): Promise<void> {
		if (!instanceId) return;
		isDeletingInstance = true;
		statusMessage = `Deleting ${instanceId}...`;
		try {
			const result = await deleteCatalogItem({ targetId: instanceId, mode: 'instance' });
			appState.selectInstance(result.nextInstanceId);
			appState.selectRun(result.nextRunId);
			appState.requestSync();
			appState.refreshNow();
			await goto(result.nextInstanceId ? `/instances/${result.nextInstanceId}` : '/instances');
		} catch (error) {
			statusMessage = error instanceof Error ? error.message : String(error);
		} finally {
			isDeletingInstance = false;
		}
	}
</script>

<section class="panel stack">
	<div class="section-title">
		<div>
			<h1>{instance?.label ?? instanceId}</h1>
			<p class="muted">Updated {formatDate(instance?.updated_at)}</p>
		</div>
		<div class="header-actions">
			<a class="action-link" href={`/instances/${instanceId}/runs`}>Runs ({instance?.run_count ?? 0})</a>
			<details class="overflow-menu">
				<summary>More</summary>
				<button type="button" class="danger-button" onclick={() => void deleteInstance()} disabled={isDeletingInstance}>
					{isDeletingInstance ? 'Deleting...' : 'Delete Instance'}
				</button>
			</details>
		</div>
	</div>

	{#if statusMessage}
		<div class="panel">{statusMessage}</div>
	{/if}

	<div class="pipeline-meta">
		<div><span>Stores</span><strong>{formatNumber(instance?.customer_count ?? null, 0)}</strong></div>
		<div><span>Demand Rows</span><strong>{formatNumber(instance?.demand_row_count ?? null, 0)}</strong></div>
		<div><span>Clustering</span><strong>{instance?.clustering_method ?? 'n/a'}</strong></div>
		<div><span>Radius</span><strong>{instance?.max_distance_from_warehouse_km ?? 'all'} km</strong></div>
		<div><span>Source Instance</span><strong>{instance?.source_instance_id ?? 'n/a'}</strong></div>
		<div><span>Runs</span><strong>{instance?.run_count ?? 0}</strong></div>
	</div>
</section>

<section class="panel stack">
	<div class="section-title">
		<div>
			<h2>Map</h2>
			<p class="muted">Use the map tabs to switch between stores, total demand, scenario demand, and clusters.</p>
		</div>
	</div>
	<SimpleMap
		data={mapData}
		summary={mapSummary}
		demandRows={mapDemandRows}
		defaultMode="clusters"
		sliceBoundaries={sliceBoundaries}
		showClusterCentroids={showCentroids}
		height={500}
	/>
</section>

<style>
	.header-actions {
		display: flex;
		gap: 0.75rem;
		align-items: end;
		flex-wrap: wrap;
	}
	.pipeline-meta {
		display: grid;
		grid-template-columns: repeat(4, minmax(0, 1fr));
		gap: 0.75rem;
	}
	.pipeline-meta div {
		border: 1px solid var(--border);
		padding: 0.75rem;
		display: grid;
		gap: 0.35rem;
	}
	.pipeline-meta span {
		font-size: 0.8rem;
		color: var(--muted);
		text-transform: uppercase;
	}
	.danger-button {
		border: 1px solid var(--border);
		background: transparent;
		padding: 0.55rem 0.8rem;
		font: inherit;
		cursor: pointer;
		color: #9b1c1c;
		width: 100%;
		text-align: left;
	}
	.overflow-menu {
		position: relative;
	}
	.overflow-menu summary {
		list-style: none;
		border: 1px solid var(--border);
		padding: 0.55rem 0.8rem;
		cursor: pointer;
	}
	.overflow-menu summary::-webkit-details-marker {
		display: none;
	}
	.overflow-menu[open] {
		z-index: 2;
	}
	.overflow-menu[open] > button {
		position: absolute;
		right: 0;
		top: calc(100% + 0.35rem);
		min-width: 12rem;
		background: var(--panel);
		box-shadow: 0 12px 30px rgba(15, 23, 42, 0.12);
	}
	@media (max-width: 760px) {
		.pipeline-meta {
			grid-template-columns: 1fr;
		}
	}
</style>
