<script lang="ts">
	import { appState } from '$lib/app-state';

	let { title = 'Upload Files', showHeader = true } = $props<{ title?: string; showHeader?: boolean }>();

	let runId = $state('');
	let kind = $state('stores');
	let instanceFile = $state<File | null>(null);
	let folderFiles = $state<FileList | null>(null);
	let uploadStatus = $state('');

	async function uploadInstanceCsv(): Promise<void> {
		if (!runId || !instanceFile) return;
		const body = new FormData();
		body.set('run_id', runId);
		body.set('kind', kind);
		body.set('file', instanceFile);
		const response = await fetch('/api/uploads/instance-csv', { method: 'POST', body });
		uploadStatus = response.ok ? 'Instance CSV uploaded.' : `Upload failed: ${response.status}`;
		appState.requestSync();
	}

	async function uploadFolder(): Promise<void> {
		if (!folderFiles?.length) return;
		let count = 0;
		for (const file of Array.from(folderFiles)) {
			const relativePath = file.webkitRelativePath || file.name;
			const body = new FormData();
			body.set('relative_path', relativePath);
			body.set('file', file);
			const response = await fetch('/api/uploads/file', { method: 'POST', body });
			if (response.ok) count += 1;
		}
		uploadStatus = `Uploaded ${count} file(s).`;
		appState.requestSync();
	}
</script>

<section class="upload-panel stack">
	{#if showHeader}
		<div class="section-title">
			<h1>{title}</h1>
			<p class="muted">Uploads go to the FastAPI file mirror. The browser sync worker then mirrors them into IndexedDB.</p>
		</div>
	{/if}

	<section class="panel stack">
		<h2>Upload Export Folder</h2>
		<p class="muted">Choose a folder that already contains server-relative paths such as `exports/...`.</p>
		<input type="file" multiple webkitdirectory onchange={(event) => (folderFiles = (event.currentTarget as HTMLInputElement).files)} />
		<button class="action" onclick={() => void uploadFolder()}>Upload Folder</button>
	</section>

	<section class="panel stack">
		<h2>Upload Instance CSV</h2>
		<label>
			<span>Run ID</span>
			<input bind:value={runId} placeholder="radius100_nested_local_c20_24" />
		</label>
		<label>
			<span>Kind</span>
			<select bind:value={kind}>
				<option value="stores">stores</option>
				<option value="demand_long">demand_long</option>
				<option value="cluster_assignments">cluster_assignments</option>
				<option value="frontend_manifest">frontend_manifest</option>
			</select>
		</label>
		<input type="file" accept=".csv,.json" onchange={(event) => (instanceFile = (event.currentTarget as HTMLInputElement).files?.[0] ?? null)} />
		<button class="action" onclick={() => void uploadInstanceCsv()}>Upload Instance File</button>
	</section>

	{#if uploadStatus}
		<div class="panel">{uploadStatus}</div>
	{/if}
</section>

<style>
	label {
		display: grid;
		gap: 0.35rem;
	}

	input,
	select {
		border: 1px solid var(--border);
		border-radius: 10px;
		padding: 0.55rem 0.7rem;
		background: transparent;
	}

	.action {
		font-weight: 600;
		cursor: pointer;
	}
</style>
