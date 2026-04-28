<script lang="ts">
	import { apiFetch } from '$lib/api';
	import { appState } from '$lib/app-state';
	import CsppParameterEditor, {
		defaultCsppRunParameters,
		serializeCsppRunParameters,
		type CsppRunParameters
	} from '../../../_components/CsppParameterEditor.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import { formatDate, formatDuration, formatNumber } from '$lib/format';
	import { estimatePipelineRuntimeFallbackSec } from '$lib/runtime-estimates';
	import { page } from '$app/state';
	import { goto } from '$app/navigation';
	import type { RuntimeInfo } from '$lib/types';
	import { deleteCatalogItem } from '$lib/run-deletion';
	import { onMount } from 'svelte';

	const instanceId = $derived(page.params.instanceId ?? null);
	let runtimes = $state<RuntimeInfo[]>([]);
	let selectedRuntimeId = $state('local');
	let statusMessage = $state('');
	let isCreatingRun = $state(false);
	let isDeletingRunId = $state<string | null>(null);
	let runParameters = $state<CsppRunParameters>(defaultCsppRunParameters());

	onMount(() => {
		appState.selectInstance(instanceId);
		void loadRuntimes();
	});

	const instance = $derived.by(() =>
		($appState.catalog?.instances ?? []).find((row) => row.instance_id === instanceId) ?? null
	);
	const activeRuntime = $derived.by(() => runtimes.find((runtime) => runtime.id === selectedRuntimeId) ?? null);
	const selectedRuntimeUsableCores = $derived.by(() => {
		const fromProbe = activeRuntime?.probe?.usable_cores;
		return typeof fromProbe === 'number' && Number.isFinite(fromProbe) ? fromProbe : null;
	});
	const selectedRuntimeEstimateSlots = $derived.by(() => {
		const cores = selectedRuntimeUsableCores ?? 1;
		return runParameters.compute_profile === 'light' ? Math.max(1, Math.floor(cores / 2)) : cores;
	});
	const estimatedRuntimeForSelectionSec = $derived.by(() =>
		estimatePipelineRuntimeFallbackSec(
			($appState.instancePayload as Record<string, unknown> | null) ?? null,
			runParameters as unknown as Record<string, unknown>,
			selectedRuntimeEstimateSlots
		)
	);
	const selectedRuntimeEstimateSummary = $derived.by(() => {
		const parts = [];
		if (estimatedRuntimeForSelectionSec != null) parts.push(`Est. ${formatDuration(estimatedRuntimeForSelectionSec)}`);
		if (selectedRuntimeEstimateSlots != null) {
			parts.push(`${formatNumber(selectedRuntimeEstimateSlots, 0)} cores`);
		} else if (estimatedRuntimeForSelectionSec != null) {
			parts.push('assuming 1 slot');
		}
		return parts.length ? parts.join(' · ') : null;
	});

	async function loadRuntimes(): Promise<void> {
		const response = await apiFetch('/api/runtimes');
		if (!response.ok) return;
		const payload = (await response.json()) as { runtimes?: RuntimeInfo[] };
		runtimes = payload.runtimes ?? [];
		if (!runtimes.some((runtime) => runtime.id === selectedRuntimeId)) {
			selectedRuntimeId = runtimes[0]?.id ?? 'local';
		}
		await probeSelectedRuntime();
	}

	async function probeSelectedRuntime(): Promise<void> {
		if (!selectedRuntimeId) return;
		try {
			const response = await apiFetch(`/api/runtimes/${encodeURIComponent(selectedRuntimeId)}/probe`, {
				method: 'POST'
			});
			if (!response.ok) return;
			const payload = (await response.json()) as { probe?: RuntimeInfo['probe']; queue?: RuntimeInfo['queue'] };
			runtimes = runtimes.map((runtime) =>
				runtime.id === selectedRuntimeId
					? {
							...runtime,
							probe: payload.probe ?? runtime.probe,
							queue: payload.queue ?? runtime.queue
						}
					: runtime
			);
		} catch {
			return;
		}
	}

	async function startRun(): Promise<void> {
		if (!instanceId) return;
		isCreatingRun = true;
		statusMessage = `Starting solve on ${selectedRuntimeId}...`;
		try {
			const form = new FormData();
			form.set('runtime_id', selectedRuntimeId);
			form.set('parameters_json', JSON.stringify(serializeCsppRunParameters(runParameters)));
			const response = await apiFetch(`/api/instances/${encodeURIComponent(instanceId)}/runs`, {
				method: 'POST',
				body: form
			});
			const text = await response.text();
			let payload: Record<string, unknown> = {};
			if (text) {
				try {
					payload = JSON.parse(text) as Record<string, unknown>;
				} catch {
					if (!response.ok) {
						throw new Error(text.trim() || `Run creation failed: ${response.status}`);
					}
					throw new Error('Run creation returned invalid JSON.');
				}
			}
			if (!response.ok) {
				throw new Error(typeof payload.detail === 'string' ? payload.detail : `Run creation failed: ${response.status}`);
			}
			const runId = typeof payload.run_id === 'string' ? payload.run_id : null;
			appState.requestSync();
			appState.refreshNow();
			if (runId) {
				appState.selectRun(runId);
				await goto(`/instances/${instanceId}/runs/${runId}`);
			}
		} catch (error) {
			statusMessage = error instanceof Error ? error.message : String(error);
		} finally {
			isCreatingRun = false;
		}
	}

	async function deleteRun(runId: string): Promise<void> {
		isDeletingRunId = runId;
		statusMessage = `Deleting ${runId}...`;
		try {
			const result = await deleteCatalogItem({ targetId: runId, mode: 'run' });
			if ($appState.selectedRunId === runId) {
				appState.selectRun(result.nextRunId);
			}
			appState.requestSync();
			appState.refreshNow();
		} catch (error) {
			statusMessage = error instanceof Error ? error.message : String(error);
		} finally {
			isDeletingRunId = null;
		}
	}

	$effect(() => {
		if (!selectedRuntimeId) return;
		void probeSelectedRuntime();
	});
</script>

<section class="panel stack">
	<div class="section-title">
		<h1>Runs</h1>
		<div class="header-actions">
			<label class="runtime-select">
				<span>Runtime</span>
				<select bind:value={selectedRuntimeId}>
					{#each runtimes as runtime}
						<option value={runtime.id}>{runtime.label ?? runtime.id}</option>
					{/each}
				</select>
			</label>
			<button type="button" class="primary-action" onclick={() => void startRun()} disabled={isCreatingRun}>
				{isCreatingRun ? 'Starting...' : 'Start New Run'}
			</button>
			{#if selectedRuntimeEstimateSummary}
				<p class="estimate-chip">{selectedRuntimeEstimateSummary}</p>
			{/if}
		</div>
	</div>

	<CsppParameterEditor value={runParameters} onchange={(value) => (runParameters = value)} compact disabled={isCreatingRun} detectedCores={selectedRuntimeUsableCores} />

	{#if statusMessage}
		<div class="panel">{statusMessage}</div>
	{/if}

	<div class="table-wrap">
		<table>
			<thead>
				<tr>
					<th>Run</th>
					<th>Status</th>
					<th>Updated</th>
					<th>Runtime</th>
					<th>Stage 1</th>
					<th>Stage 2</th>
					<th>Stage 3</th>
					<th>Actions</th>
				</tr>
			</thead>
			<tbody>
				{#each instance?.runs ?? [] as run}
					<tr>
						<td><a href={`/instances/${instanceId}/runs/${run.run_id}`}>{run.run_id}</a></td>
						<td><StatusPill value={run.status ?? 'idle'} /></td>
						<td>{formatDate(run.run_last_modified_at)}</td>
						<td>{run.runtime_label ?? run.runtime_id ?? 'n/a'}</td>
						<td><StatusPill value={run.stage_status?.first_stage ?? 'missing'} /></td>
						<td><StatusPill value={run.stage_status?.scenario_evaluation ?? 'missing'} /></td>
						<td><StatusPill value={run.stage_status?.reoptimization ?? 'missing'} /></td>
						<td>
							<details class="overflow-menu">
								<summary>More</summary>
								<button type="button" class="danger-button" onclick={() => void deleteRun(run.run_id)} disabled={isDeletingRunId === run.run_id}>
									{isDeletingRunId === run.run_id ? 'Deleting...' : 'Delete Run'}
								</button>
							</details>
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
</section>

<style>
	.header-actions {
		display: flex;
		gap: 0.75rem;
		align-items: end;
		flex-wrap: wrap;
	}
	.runtime-select {
		display: grid;
		gap: 0.3rem;
		font-size: 0.8rem;
		color: var(--muted);
	}
	.runtime-select select {
		min-width: 10rem;
	}
	.primary-action,
	.danger-button {
		border: 1px solid var(--border);
		background: transparent;
		padding: 0.55rem 0.8rem;
		font: inherit;
		cursor: pointer;
	}
	.danger-button {
		color: #9b1c1c;
		width: 100%;
		text-align: left;
	}
	.overflow-menu {
		position: relative;
		display: inline-block;
	}
	.overflow-menu summary {
		list-style: none;
		border: 1px solid var(--border);
		padding: 0.4rem 0.65rem;
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
	.estimate-chip {
		margin: 0;
		padding: 0.55rem 0.8rem;
		border: 1px solid var(--border);
		background: color-mix(in srgb, var(--panel) 82%, transparent);
		font-size: 0.85rem;
		color: var(--muted);
		white-space: nowrap;
	}
</style>
