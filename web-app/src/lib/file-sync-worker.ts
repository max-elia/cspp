import { apiFetch, apiPath } from '$lib/api';
import { deleteMirroredFile, getMeta, putMeta, putMirroredFile } from '$lib/file-mirror-db';

type ServerFile = {
	path: string;
	size: number;
	mtime_ms: number;
	sha256: string;
	content_type: string;
};

type ServerIndex = {
	updated_at: string;
	files: ServerFile[];
};

const INTERVAL_MS = 10_000;
let timer: ReturnType<typeof setInterval> | null = null;
let inFlight = false;

async function syncOnce(): Promise<void> {
	if (inFlight) return;
	inFlight = true;
	try {
		const response = await apiFetch(`/api/sync/index?t=${Date.now()}`, { cache: 'no-store' });
		if (!response.ok) throw new Error(`Index request failed with ${response.status}`);
		const index = (await response.json()) as ServerIndex;
		const previous = (await getMeta<Record<string, { sha256: string }>>('file-index')) ?? {};
		const nextIndex: Record<string, { sha256: string }> = {};
		const seen = new Set<string>();

		for (const file of index.files) {
			seen.add(file.path);
			nextIndex[file.path] = { sha256: file.sha256 };
			if (previous[file.path]?.sha256 === file.sha256) continue;
			const fileResponse = await fetch(apiPath(`/api/sync/file?path=${encodeURIComponent(file.path)}&t=${Date.now()}`), {
				cache: 'no-store'
			});
			if (!fileResponse.ok) continue;
			const blob = await fileResponse.blob();
			await putMirroredFile({
				path: file.path,
				blob,
				contentType: file.content_type,
				size: file.size,
				sha256: file.sha256,
				mtimeMs: file.mtime_ms,
				syncedAt: new Date().toISOString()
			});
		}

		for (const path of Object.keys(previous)) {
			if (!seen.has(path)) {
				await deleteMirroredFile(path);
			}
		}

		await putMeta('file-index', nextIndex);
		await putMeta('sync-status', {
			lastSuccessfulAt: new Date().toISOString(),
			fileCount: index.files.length,
			inFlight: false,
			error: null
		});
		self.postMessage({ type: 'synced', fileCount: index.files.length });
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		await putMeta('sync-status', {
			lastSuccessfulAt: null,
			fileCount: 0,
			inFlight: false,
			error: message
		});
		self.postMessage({ type: 'error', message });
	} finally {
		inFlight = false;
	}
}

self.onmessage = (event: MessageEvent<{ type: string }>) => {
	if (event.data.type === 'start' && !timer) {
		void syncOnce();
		timer = setInterval(() => void syncOnce(), INTERVAL_MS);
	}
	if (event.data.type === 'sync-now') {
		void syncOnce();
	}
	if (event.data.type === 'stop' && timer) {
		clearInterval(timer);
		timer = null;
	}
};

export {};
