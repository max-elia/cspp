export function formatNumber(value: unknown, digits = 2): string {
	if (value === null || value === undefined || value === '') return 'n/a';
	const num = Number(value);
	if (Number.isNaN(num)) return String(value);
	if (Number.isInteger(num)) return new Intl.NumberFormat().format(num);
	return new Intl.NumberFormat(undefined, { maximumFractionDigits: digits }).format(num);
}

export function formatDate(value: unknown): string {
	if (!value) return 'n/a';
	const date = new Date(String(value));
	if (Number.isNaN(date.getTime())) return String(value);
	return date.toLocaleString();
}

export function formatDuration(value: unknown): string {
	if (value === null || value === undefined || value === '') return 'n/a';
	const totalSeconds = Math.max(0, Math.floor(Number(value)));
	if (Number.isNaN(totalSeconds)) return String(value);

	const days = Math.floor(totalSeconds / 86400);
	const hours = Math.floor((totalSeconds % 86400) / 3600);
	const minutes = Math.floor((totalSeconds % 3600) / 60);
	const seconds = totalSeconds % 60;
	const hhmmss = [hours, minutes, seconds].map((part) => String(part).padStart(2, '0')).join(':');
	return days > 0 ? `${days}d ${hhmmss}` : hhmmss;
}

export function titleCase(value: string): string {
	return value.replace(/[_-]+/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase());
}
