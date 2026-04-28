<script lang="ts">
	import { appState } from '$lib/app-state';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import { formatDate } from '$lib/format';
</script>

<section class="panel stack">
	<div class="section-title">
		<h1>Instances</h1>
		<a class="action-link" href="/instances/new">+ New Instance</a>
	</div>

	<div class="table-wrap">
		<table>
			<thead>
				<tr>
					<th>Instance</th>
					<th>Updated</th>
					<th>Clustering</th>
					<th>Radius</th>
					<th>Stores</th>
					<th>Runs</th>
					<th>Latest Run</th>
				</tr>
			</thead>
			<tbody>
				{#each $appState.catalog?.instances ?? [] as instance}
					<tr>
						<td><a href={`/instances/${instance.instance_id}`}>{instance.label ?? instance.instance_id}</a></td>
						<td>{formatDate(instance.updated_at)}</td>
						<td>{instance.clustering_method ?? 'n/a'}</td>
						<td>{instance.max_distance_from_warehouse_km ?? 'all'}</td>
						<td>{instance.customer_count ?? 'n/a'}</td>
						<td>{instance.run_count ?? 0}</td>
						<td>
							{#if instance.runs?.[0]}
								<div>{instance.runs[0].run_id}</div>
								<StatusPill value={instance.runs[0].status ?? 'idle'} />
							{:else}
								<span class="muted">No runs</span>
							{/if}
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
</section>
