<script lang="ts">
	import type { DeleteMode } from '$lib/run-deletion';

	type Props = {
		runId: string;
		isDeleting?: boolean;
		onCancel: () => void;
		onConfirm: (mode: DeleteMode) => void;
	};

	let { runId, isDeleting = false, onCancel, onConfirm }: Props = $props();

	let mode = $state<DeleteMode>('run');

	$effect(() => {
		runId;
		mode = 'run';
	});
</script>

<div
	class="modal-backdrop"
	role="presentation"
	tabindex="-1"
	onclick={() => !isDeleting && onCancel()}
	onkeydown={(event) => {
		if (event.key === 'Escape' && !isDeleting) onCancel();
	}}
>
	<div
		class="modal-card"
		role="dialog"
		tabindex="-1"
		aria-modal="true"
		aria-labelledby="delete-instance-title"
		onclick={(event) => event.stopPropagation()}
		onkeydown={(event) => event.stopPropagation()}
	>
		<h2 id="delete-instance-title">Delete Instance</h2>
		<p class="muted">Choose what should be deleted for <strong>{runId}</strong>.</p>

		<div class="option-list">
			<label class="option-row">
				<input type="radio" bind:group={mode} value="run" disabled={isDeleting} />
				<div>
					<div class="option-title">Run Only</div>
					<div class="option-copy">Deletes only the selected run.</div>
				</div>
			</label>

			<label class="option-row">
				<input type="radio" bind:group={mode} value="instance" disabled={isDeleting} />
				<div>
					<div class="option-title">Whole Instance</div>
					<div class="option-copy">Deletes the instance and all attached runs.</div>
				</div>
			</label>
		</div>

		<div class="actions">
			<button type="button" class="secondary-button" onclick={onCancel} disabled={isDeleting}>Cancel</button>
			<button type="button" class="danger-button" onclick={() => onConfirm(mode)} disabled={isDeleting}>
				{isDeleting ? 'Deleting...' : 'Delete'}
			</button>
		</div>
	</div>
</div>

<style>
	.modal-backdrop {
		position: fixed;
		inset: 0;
		background: rgb(0 0 0 / 0.34);
		display: grid;
		place-items: center;
		padding: 1rem;
		z-index: 1000;
	}

	.modal-card {
		width: min(32rem, 100%);
		background: var(--bg, #fff);
		border: 1px solid var(--border);
		padding: 1rem;
		display: grid;
		gap: 0.9rem;
		box-shadow: 0 12px 30px rgb(0 0 0 / 0.14);
	}

	h2 {
		margin: 0;
		font-size: 1rem;
	}

	.option-list {
		display: grid;
		gap: 0.65rem;
	}

	.option-row {
		display: grid;
		grid-template-columns: auto 1fr;
		gap: 0.75rem;
		align-items: start;
		padding: 0.75rem;
		border: 1px solid var(--border);
		cursor: pointer;
	}

	.option-title {
		font-size: 0.92rem;
		font-weight: 600;
	}

	.option-copy {
		font-size: 0.84rem;
		color: var(--muted);
	}

	.actions {
		display: flex;
		justify-content: flex-end;
		gap: 0.6rem;
	}

	.secondary-button,
	.danger-button {
		border: 1px solid var(--border);
		background: transparent;
		padding: 0.35rem 0.6rem;
		border-radius: 0;
		font: inherit;
		font-size: 0.82rem;
		line-height: 1.2;
		cursor: pointer;
	}

	.danger-button {
		color: #9b1c1c;
	}

	button:disabled {
		opacity: 0.6;
		cursor: default;
	}
</style>
