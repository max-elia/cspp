export type MirroredFile = {
	path: string;
	blob: Blob;
	contentType: string;
	size: number;
	sha256: string;
	mtimeMs: number;
	syncedAt: string;
};

const DB_NAME = 'thesis-file-mirror';
const DB_VERSION = 1;
const FILES_STORE = 'files';
const META_STORE = 'meta';

let dbPromise: Promise<IDBDatabase> | null = null;

function openDb(): Promise<IDBDatabase> {
	if (dbPromise) return dbPromise;
	dbPromise = new Promise((resolve, reject) => {
		const request = indexedDB.open(DB_NAME, DB_VERSION);
		request.onupgradeneeded = () => {
			const db = request.result;
			if (!db.objectStoreNames.contains(FILES_STORE)) {
				db.createObjectStore(FILES_STORE, { keyPath: 'path' });
			}
			if (!db.objectStoreNames.contains(META_STORE)) {
				db.createObjectStore(META_STORE, { keyPath: 'key' });
			}
		};
		request.onsuccess = () => resolve(request.result);
		request.onerror = () => reject(request.error);
	});
	return dbPromise;
}

function requestToPromise<T>(request: IDBRequest<T>): Promise<T> {
	return new Promise((resolve, reject) => {
		request.onsuccess = () => resolve(request.result);
		request.onerror = () => reject(request.error);
	});
}

export async function getMirroredFile(path: string): Promise<MirroredFile | null> {
	const db = await openDb();
	const tx = db.transaction(FILES_STORE, 'readonly');
	return (await requestToPromise(tx.objectStore(FILES_STORE).get(path))) ?? null;
}

export async function getMirroredText(path: string): Promise<string | null> {
	const record = await getMirroredFile(path);
	if (!record) return null;
	return await record.blob.text();
}

export async function getMirroredJson<T>(path: string): Promise<T | null> {
	const text = await getMirroredText(path);
	if (!text) return null;
	try {
		return JSON.parse(text) as T;
	} catch {
		return null;
	}
}

export async function putMirroredFile(record: MirroredFile): Promise<void> {
	const db = await openDb();
	const tx = db.transaction(FILES_STORE, 'readwrite');
	await requestToPromise(tx.objectStore(FILES_STORE).put(record));
}

export async function deleteMirroredFile(path: string): Promise<void> {
	const db = await openDb();
	const tx = db.transaction(FILES_STORE, 'readwrite');
	await requestToPromise(tx.objectStore(FILES_STORE).delete(path));
}

export async function putMeta<T>(key: string, value: T): Promise<void> {
	const db = await openDb();
	const tx = db.transaction(META_STORE, 'readwrite');
	await requestToPromise(tx.objectStore(META_STORE).put({ key, value }));
}

export async function getMeta<T>(key: string): Promise<T | null> {
	const db = await openDb();
	const tx = db.transaction(META_STORE, 'readonly');
	const result = await requestToPromise<{ key: string; value: T } | undefined>(tx.objectStore(META_STORE).get(key));
	return result?.value ?? null;
}

export async function listMirroredPaths(prefix = ''): Promise<string[]> {
	const db = await openDb();
	const tx = db.transaction(FILES_STORE, 'readonly');
	const store = tx.objectStore(FILES_STORE);
	const request = store.getAllKeys();
	const keys = await requestToPromise<IDBValidKey[]>(request);
	return keys.map(String).filter((key) => key.startsWith(prefix)).sort();
}
