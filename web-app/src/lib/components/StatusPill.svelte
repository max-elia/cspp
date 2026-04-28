<script lang="ts">
	let { value = 'unknown' } = $props<{ value?: string }>();
</script>

<span class={`pill ${String(value).toLowerCase()}`}>{value}</span>

<style>
	.pill {
		display: inline-flex;
		align-items: center;
		border: 1px solid var(--border);
		padding: 0.2rem 0.5rem;
		font-size: 0.75rem;
		font-weight: 600;
		color: var(--text);
		background: #f7f7f7;
	}
	.not.started, .idle, .unknown {
		background: #f3f3f3;
		border-color: var(--border);
		color: var(--muted);
	}
	.running, .initialized, .live {
		background: #fff2b8;
		border-color: #c9b458;
		color: #6d5200;
	}
	.timeout, .warning {
		background: #ffe2c7;
		border-color: #c28c4c;
		color: #854400;
	}
	.failed, .error, .missing {
		background: #ffe0e0;
		border-color: #b46a6a;
		color: #8a1c1c;
	}
	.completed, .optimal, .feasible {
		background: #dff4e5;
		border-color: #77a07f;
		color: #1f5d30;
	}
	.processing, .queued {
		background: #e4eefc;
		border-color: #7aa2e3;
		color: #184a97;
	}
</style>
