const rawApiBase = (import.meta.env.PUBLIC_API_BASE_URL ?? '').trim();

function normalizeApiBase(value: string): string {
	if (!value) return '';
	return value.endsWith('/') ? value.slice(0, -1) : value;
}

const apiBase = normalizeApiBase(rawApiBase || (import.meta.env.DEV ? 'http://127.0.0.1:8000' : ''));

export function apiPath(path: string): string {
	if (/^https?:\/\//i.test(path)) return path;
	if (!path.startsWith('/')) {
		throw new Error(`API path must start with '/': ${path}`);
	}
	return `${apiBase}${path}`;
}

export function apiFetch(input: string, init?: RequestInit): Promise<Response> {
	return fetch(apiPath(input), init);
}
