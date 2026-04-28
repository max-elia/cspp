<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { appState } from '$lib/app-state';
	import SimpleMap from '$lib/components/SimpleMap.svelte';
	import { formatNumber } from '$lib/format';
	import { deleteCatalogItem } from '$lib/run-deletion';
	import type { AssignmentRow, ClusterCentroid, CustomerRecord } from '$lib/clustering-browser';
	import type { FeatureCollection, MapDemandRow } from '$lib/types';
	import { onMount } from 'svelte';

	const instanceId = $derived(page.params.instanceId ?? null);
	const runId = $derived(page.params.runId ?? null);

	onMount(() => {
		appState.selectRun(runId);
		appState.ensureMapData();
	});

	const summary = $derived($appState.mapSummary as Record<string, unknown> | null);
	const mapData = $derived($appState.mapData as FeatureCollection | null);
	const mapDemandRows = $derived($appState.mapDemandRows as MapDemandRow[]);
	const features = $derived(mapData?.features ?? []);

	function clusterValue(value: unknown): number | null {
		if (value === null || value === undefined || value === '') return null;
		const number = Number(value);
		return Number.isFinite(number) ? number : null;
	}

	function buildCustomerRecords(sourceFeatures: FeatureCollection['features']): CustomerRecord[] {
		return sourceFeatures.map((feature) => {
			const properties = feature.properties as Record<string, unknown>;
			return {
				client_num: Number(properties.client_num ?? 0),
				customer_id: (properties.customer_id as string | null | undefined) ?? null,
				customer_name: (properties.customer_name as string | null | undefined) ?? null,
				latitude: feature.geometry.coordinates[1],
				longitude: feature.geometry.coordinates[0],
				cluster_id: clusterValue(properties.cluster_id),
				super_cluster_id: clusterValue(properties.super_cluster_id),
				total_demand_kg: Number(properties.total_demand ?? 0)
			};
		});
	}

	function buildCentroidsFromAssignments(customers: CustomerRecord[], rows: AssignmentRow[]): ClusterCentroid[] {
		const grouped = new Map<number, { latitude: number; longitude: number; size: number }>();
		const customersByClient = new Map(customers.map((customer) => [customer.client_num, customer]));
		for (const row of rows) {
			const customer = customersByClient.get(row.client_num);
			if (!customer) continue;
			const current = grouped.get(row.cluster_id) ?? { latitude: 0, longitude: 0, size: 0 };
			current.latitude += customer.latitude;
			current.longitude += customer.longitude;
			current.size += 1;
			grouped.set(row.cluster_id, current);
		}
		return Array.from(grouped.entries())
			.sort((a, b) => a[0] - b[0])
			.map(([cluster_id, value]) => ({
				cluster_id,
				latitude: value.latitude / Math.max(1, value.size),
				longitude: value.longitude / Math.max(1, value.size),
				size: value.size
			}));
	}

	const currentRunCustomers = $derived.by(() => buildCustomerRecords(features));
	const confirmedAssignments = $derived.by(() =>
		currentRunCustomers
			.filter((customer) => customer.cluster_id !== null && customer.cluster_id !== undefined)
			.map((customer) => ({
				client_num: customer.client_num,
				customer_id: customer.customer_id ?? undefined,
				cluster_id: Number(customer.cluster_id ?? 0),
				super_cluster_id: Number(customer.super_cluster_id ?? 0)
			}))
	);
	const confirmedCentroids = $derived.by(() => buildCentroidsFromAssignments(currentRunCustomers, confirmedAssignments));
	const confirmedClusterMap = $derived.by(() => new Map(confirmedAssignments.map((row) => [row.client_num, row.cluster_id])));
	const clusterCount = $derived.by(() => new Set(confirmedAssignments.map((row) => row.cluster_id)).size);
	const demandDateCount = $derived.by(() => (Array.isArray(summary?.available_demand_dates) ? summary.available_demand_dates.length : 0));

	let deleteStatus = $state('');
	let isDeleting = $state(false);
	async function handleDeleteCurrentRun(): Promise<void> {
		if (!runId) return;
		isDeleting = true;
		deleteStatus = `Deleting ${runId}...`;

		try {
			const result = await deleteCatalogItem({ targetId: runId, mode: 'run' });
			appState.selectRun(result.nextRunId);
			appState.refreshNow();
			appState.requestSync();
			await goto(instanceId ? `/instances/${instanceId}/runs` : '/instances');
		} catch (error) {
			deleteStatus = error instanceof Error ? error.message : String(error);
		} finally {
			isDeleting = false;
		}
	}
</script>

<section class="panel stack">
	<div class="section-title">
		<h1>Map</h1>
		<div class="actions-row">
			<a class="action-link" href="/instances/new">+ New Instance</a>
			<details class="overflow-menu">
				<summary>More</summary>
				<button type="button" class="danger-button" onclick={() => void handleDeleteCurrentRun()} disabled={isDeleting}>
					{isDeleting ? 'Deleting...' : 'Delete Run'}
				</button>
			</details>
		</div>
	</div>

	{#if deleteStatus}
		<div class="panel status-panel">{deleteStatus}</div>
	{/if}

	<div class="workspace-layout">
		<div class="map-column">
			<SimpleMap
				data={mapData}
				summary={summary}
				demandRows={mapDemandRows}
				clusterAssignments={confirmedClusterMap}
				clusterCentroids={confirmedCentroids}
				showClusterCentroids={false}
				height={500}
			/>
		</div>

			<aside class="info-rail">
				<div class="rail-section">
					<div class="rail-title">Instance</div>
					<div class="kv"><span>Stores</span><strong>{formatNumber(summary?.customer_count, 0)}</strong></div>
					<div class="kv"><span>Clusters</span><strong>{formatNumber(clusterCount, 0)}</strong></div>
				</div>

			<div class="rail-section">
				<div class="rail-title">Demand</div>
				<div class="kv"><span>Min</span><strong>{formatNumber(summary?.demand_min, 0)}</strong></div>
				<div class="kv"><span>Max</span><strong>{formatNumber(summary?.demand_max, 0)}</strong></div>
				<div class="kv"><span>Scenarios</span><strong>{formatNumber(demandDateCount, 0)}</strong></div>
			</div>
		</aside>
	</div>
</section>

<style>
	.actions-row {
		display: flex;
		gap: 0.75rem;
		align-items: center;
	}

	.status-panel {
		padding: 0.85rem 1rem;
	}

	.danger-button {
		border: 1px solid var(--border);
		background: transparent;
		color: #9b1c1c;
		padding: 0.2rem 0.45rem;
		border-radius: 0;
		font-size: 0.8rem;
		line-height: 1.2;
		cursor: pointer;
		font: inherit;
		width: 100%;
		text-align: left;
	}

	.danger-button:disabled {
		opacity: 0.6;
		cursor: default;
	}

	.overflow-menu {
		position: relative;
	}

	.overflow-menu summary {
		list-style: none;
		border: 1px solid var(--border);
		padding: 0.45rem 0.7rem;
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
		min-width: 10rem;
		background: var(--panel);
		box-shadow: 0 12px 30px rgba(15, 23, 42, 0.12);
	}

	.workspace-layout {
		display: grid;
		grid-template-columns: minmax(0, 1fr) 16rem;
		gap: 1rem;
		align-items: start;
	}

	.map-column {
		min-width: 0;
		--map-header-offset: 5.5rem;
	}

	.info-rail {
		display: grid;
		gap: 0.8rem;
		align-self: start;
		margin-top: var(--map-header-offset);
	}

	.rail-section {
		border: 1px solid var(--border);
		padding: 0.85rem 0.9rem;
		display: grid;
		gap: 0.55rem;
	}

	.rail-title {
		font-size: 0.78rem;
		font-weight: 700;
		letter-spacing: 0.02em;
		text-transform: uppercase;
		color: var(--muted);
	}

	.kv {
		display: flex;
		justify-content: space-between;
		gap: 0.75rem;
		font-size: 0.9rem;
	}

	.kv span {
		color: var(--muted);
	}

	@media (max-width: 760px) {
		.workspace-layout {
			grid-template-columns: 1fr;
		}

		.info-rail {
			margin-top: 0;
		}
	}
</style>
