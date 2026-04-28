<script module lang="ts">
	export type CsppRunParameters = {
		d_cost: number;
		h: number;
		max_tours_per_truck: number;
		charger_cost_multiplier: number;
		vehicle_type: 'mercedes' | 'volvo';
		compute_profile: 'light' | 'heavy';
		stage1_max_iterations: number | null;
		reopt_max_iterations: number | null;
		gap: number | null;
		reopt_eval_mipgap: number | null;
	};

	const DEFAULT_PARAMETERS: CsppRunParameters = {
		d_cost: 0.25,
		h: 50,
		max_tours_per_truck: 3,
		charger_cost_multiplier: 1,
		vehicle_type: 'mercedes',
		compute_profile: 'heavy',
		stage1_max_iterations: null,
		reopt_max_iterations: null,
		gap: null,
		reopt_eval_mipgap: 0.05
	};

	const CHARGER_TYPES = [
		{ id: 1, name: '22 kW AC', cost: 7500, mercedes: true, volvo: true },
		{ id: 2, name: '43 kW AC', cost: 8500, mercedes: false, volvo: true },
		{ id: 3, name: '40 kW DC', cost: 22500, mercedes: true, volvo: true },
		{ id: 4, name: '50 kW DC', cost: 33000, mercedes: true, volvo: true },
		{ id: 5, name: '90 kW DC', cost: 60000, mercedes: true, volvo: true },
		{ id: 6, name: '120 kW DC', cost: 75000, mercedes: true, volvo: true },
		{ id: 7, name: '150 kW DC', cost: 100000, mercedes: true, volvo: true },
		{ id: 8, name: '250 kW DC', cost: 130000, mercedes: false, volvo: true }
	];

	export function defaultCsppRunParameters(): CsppRunParameters {
		return { ...DEFAULT_PARAMETERS };
	}

	export function normalizeCsppRunParameters(source: Record<string, unknown> | null | undefined): CsppRunParameters {
		return {
			...DEFAULT_PARAMETERS,
			d_cost: finiteNumber(source?.d_cost, DEFAULT_PARAMETERS.d_cost),
			h: finiteNumber(source?.h, DEFAULT_PARAMETERS.h),
			max_tours_per_truck: finiteInteger(source?.max_tours_per_truck, DEFAULT_PARAMETERS.max_tours_per_truck),
			charger_cost_multiplier: finiteNumber(
				source?.charger_cost_multiplier,
				DEFAULT_PARAMETERS.charger_cost_multiplier
			),
			vehicle_type: source?.vehicle_type === 'volvo' ? 'volvo' : 'mercedes',
			compute_profile: source?.compute_profile === 'light' ? 'light' : 'heavy',
			stage1_max_iterations: nullableInteger(source?.stage1_max_iterations),
			reopt_max_iterations: nullableInteger(source?.reopt_max_iterations),
			gap: nullableNumber(source?.gap),
			reopt_eval_mipgap: nullableNumber(source?.reopt_eval_mipgap) ?? 0.05
		};
	}

	export function serializeCsppRunParameters(parameters: CsppRunParameters): Record<string, number | string> {
		const payload: Record<string, number | string> = {
			d_cost: parameters.d_cost,
			h: parameters.h,
			max_tours_per_truck: parameters.max_tours_per_truck,
			charger_cost_multiplier: parameters.charger_cost_multiplier,
			vehicle_type: parameters.vehicle_type,
			compute_profile: parameters.compute_profile,
			reopt_eval_mipgap: parameters.reopt_eval_mipgap ?? 0.05
		};
		for (const key of ['stage1_max_iterations', 'reopt_max_iterations'] as const) {
			const fieldValue = parameters[key];
			if (fieldValue !== null) payload[key] = fieldValue;
		}
		if (parameters.gap !== null) payload.gap = parameters.gap;
		return payload;
	}

	function finiteNumber(source: unknown, fallback: number): number {
		const parsed = Number(source);
		return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
	}

	function finiteInteger(source: unknown, fallback: number): number {
		const parsed = Math.round(Number(source));
		return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
	}

	function nullableNumber(source: unknown): number | null {
		if (source === null || source === undefined || source === '') return null;
		const parsed = Number(source);
		return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
	}

	function nullableInteger(source: unknown): number | null {
		const parsed = nullableNumber(source);
		return parsed === null ? null : Math.round(parsed);
	}
</script>

<script lang="ts">
	import { formatNumber } from '$lib/format';

	type Props = {
		value: CsppRunParameters;
		onchange: (value: CsppRunParameters) => void;
		compact?: boolean;
		disabled?: boolean;
		detectedCores?: number | null;
	};

	let { value, onchange, compact = false, disabled = false, detectedCores = null }: Props = $props();

	function update<K extends keyof CsppRunParameters>(key: K, nextValue: CsppRunParameters[K]): void {
		onchange({ ...value, [key]: nextValue });
	}

	function updateNumber(key: keyof CsppRunParameters, rawValue: string, nullable = false): void {
		const parsed = rawValue === '' && nullable ? null : Number(rawValue);
		if (parsed !== null && (!Number.isFinite(parsed) || parsed < 0)) return;
		update(key, parsed as never);
	}

	const computeCores = $derived.by(() => {
		const cores = typeof detectedCores === 'number' && Number.isFinite(detectedCores) ? Math.max(1, Math.floor(detectedCores)) : null;
		if (cores === null) return value.compute_profile === 'light' ? 'light' : 'detected';
		return value.compute_profile === 'light' ? Math.max(1, Math.floor(cores / 2)) : cores;
	});
	const headline = $derived(
		`${formatNumber(value.d_cost, 2)} EUR/kWh · ${formatNumber(value.h, 0)} EUR/h · ${computeCores} cores`
	);
	const availableChargers = $derived(
		CHARGER_TYPES.filter((charger) => value.vehicle_type === 'volvo' ? charger.volvo : charger.mercedes)
	);
	const chargerCostSummary = $derived(
		availableChargers.map((charger) => `${charger.name}: ${formatNumber(charger.cost * value.charger_cost_multiplier, 0)} EUR`).join(' · ')
	);

	const helpText = {
		d_cost: 'Electricity price used for charging cost in all CSPP objective terms.',
		h: 'Cost of waiting time in EUR per hour when vehicles charge during operations.',
		max_tours_per_truck: 'Upper bound on the number of tours a truck may perform in one scenario.',
		charger_cost_multiplier: 'Multiplier applied to the fixed charger installation costs listed below.',
		vehicle_type: 'Vehicle energy model and compatible charger set used by the solver.',
		compute_profile: 'Heavy uses the full detected usable core budget; Light uses roughly half for a less intrusive run.',
		stage1_max_iterations: 'Optional cap for Stage 1 cluster master iterations. Empty uses the algorithm default.',
		reopt_max_iterations: 'Optional cap for Stage 3 reoptimization iterations. Empty uses all available scenario-driven iterations.',
		gap: 'Optional MIP optimality gap for solver calls that use the shared gap parameter.',
		reopt_eval_mipgap: 'MIP gap used for scenario evaluations inside Stage 3 reoptimization.'
	};
</script>

<div class:compact class="parameter-editor">
	<div class="parameter-head">
		<div>
			<h3>Run Parameters</h3>
			<p class="muted">{headline}</p>
		</div>
	</div>

	<div class="parameter-grid">
		<label>
			<span class="field-label">Energy Cost <button class="info-dot" type="button" aria-label="Parameter information">i<span class="tooltip">{helpText.d_cost}</span></button></span>
			<input type="number" min="0" step="0.01" value={value.d_cost} {disabled} oninput={(event) => updateNumber('d_cost', event.currentTarget.value)} />
		</label>
		<label>
			<span class="field-label">Waiting Cost <button class="info-dot" type="button" aria-label="Parameter information">i<span class="tooltip">{helpText.h}</span></button></span>
			<input type="number" min="0" step="1" value={value.h} {disabled} oninput={(event) => updateNumber('h', event.currentTarget.value)} />
		</label>
		<label>
			<span class="field-label">Max Tours <button class="info-dot" type="button" aria-label="Parameter information">i<span class="tooltip">{helpText.max_tours_per_truck}</span></button></span>
			<input type="number" min="0" step="1" value={value.max_tours_per_truck} {disabled} oninput={(event) => updateNumber('max_tours_per_truck', event.currentTarget.value)} />
		</label>
		<label>
			<span class="field-label">Charger Cost Multiplier <button class="info-dot" type="button" aria-label="Parameter information">i<span class="tooltip">{helpText.charger_cost_multiplier}</span></button></span>
			<input type="number" min="0" step="0.05" value={value.charger_cost_multiplier} {disabled} oninput={(event) => updateNumber('charger_cost_multiplier', event.currentTarget.value)} />
		</label>
		<label>
			<span class="field-label">Vehicle <button class="info-dot" type="button" aria-label="Parameter information">i<span class="tooltip">{helpText.vehicle_type}</span></button></span>
			<select value={value.vehicle_type} {disabled} onchange={(event) => update('vehicle_type', event.currentTarget.value === 'volvo' ? 'volvo' : 'mercedes')}>
				<option value="mercedes">Mercedes eActros</option>
				<option value="volvo">Volvo FM Electric</option>
			</select>
		</label>
		<label>
			<span class="field-label">Compute <button class="info-dot" type="button" aria-label="Parameter information">i<span class="tooltip">{helpText.compute_profile}</span></button></span>
			<select value={value.compute_profile} {disabled} onchange={(event) => update('compute_profile', event.currentTarget.value === 'light' ? 'light' : 'heavy')}>
				<option value="heavy">Heavy ({detectedCores ? formatNumber(detectedCores, 0) : 'detected'} cores)</option>
				<option value="light">Light ({detectedCores ? formatNumber(Math.max(1, Math.floor(detectedCores / 2)), 0) : 'fewer'} cores)</option>
			</select>
		</label>
		<label>
			<span class="field-label">Stage 1 Iter. <button class="info-dot" type="button" aria-label="Parameter information">i<span class="tooltip">{helpText.stage1_max_iterations}</span></button></span>
			<input type="number" min="0" step="1" placeholder="default" value={value.stage1_max_iterations ?? ''} {disabled} oninput={(event) => updateNumber('stage1_max_iterations', event.currentTarget.value, true)} />
		</label>
		<label>
			<span class="field-label">Reopt Iter. <button class="info-dot" type="button" aria-label="Parameter information">i<span class="tooltip">{helpText.reopt_max_iterations}</span></button></span>
			<input type="number" min="0" step="1" placeholder="default" value={value.reopt_max_iterations ?? ''} {disabled} oninput={(event) => updateNumber('reopt_max_iterations', event.currentTarget.value, true)} />
		</label>
		<label>
			<span class="field-label">MIP Gap <button class="info-dot" type="button" aria-label="Parameter information">i<span class="tooltip">{helpText.gap}</span></button></span>
			<input type="number" min="0" step="0.01" placeholder="default" value={value.gap ?? ''} {disabled} oninput={(event) => updateNumber('gap', event.currentTarget.value, true)} />
		</label>
		<label>
			<span class="field-label">Stage 3 Eval Gap <button class="info-dot" type="button" aria-label="Parameter information">i<span class="tooltip">{helpText.reopt_eval_mipgap}</span></button></span>
			<input type="number" min="0" step="0.01" value={value.reopt_eval_mipgap ?? 0.05} {disabled} oninput={(event) => updateNumber('reopt_eval_mipgap', event.currentTarget.value, true)} />
		</label>
	</div>
	<p class="charger-costs">Algorithm charger costs: {chargerCostSummary}</p>
</div>

<style>
	.parameter-editor {
		border: 1px solid var(--border);
		padding: 0.65rem;
		display: grid;
		gap: 0.55rem;
	}

	.parameter-editor.compact {
		max-width: none;
	}

	.parameter-head {
		display: flex;
		justify-content: space-between;
		gap: 1rem;
		align-items: start;
	}

	.parameter-head h3,
	.parameter-head p {
		margin: 0;
	}

	.parameter-grid {
		display: grid;
		grid-template-columns: repeat(5, minmax(7rem, 1fr));
		gap: 0.5rem;
	}

	.charger-costs {
		margin: 0;
		color: var(--muted);
		font-size: 0.76rem;
		line-height: 1.35;
	}

	label {
		display: grid;
		gap: 0.22rem;
		font-size: 0.76rem;
		color: var(--muted);
		min-width: 0;
	}

	.field-label {
		display: inline-flex;
		align-items: center;
		gap: 0.28rem;
		min-width: 0;
	}

	.info-dot {
		position: relative;
		display: inline-flex;
		align-items: center;
		justify-content: center;
		width: 1rem;
		height: 1rem;
		border: 1px solid var(--border-strong);
		border-radius: 50%;
		color: var(--text);
		font-size: 0.68rem;
		font-weight: 700;
		line-height: 1;
		cursor: help;
		flex: 0 0 auto;
	}

	.tooltip {
		position: absolute;
		left: 50%;
		bottom: calc(100% + 0.45rem);
		transform: translateX(-50%);
		width: min(16rem, 70vw);
		padding: 0.45rem 0.55rem;
		border: 1px solid var(--border-strong);
		background: #fff;
		color: var(--text);
		box-shadow: 0 10px 26px rgba(15, 23, 42, 0.12);
		font-size: 0.76rem;
		font-weight: 400;
		line-height: 1.35;
		opacity: 0;
		pointer-events: none;
		z-index: 10;
	}

	.info-dot:hover .tooltip,
	.info-dot:focus .tooltip,
	.info-dot:focus-visible .tooltip {
		opacity: 1;
	}

	label input,
	label select {
		width: 100%;
		box-sizing: border-box;
		color: var(--text);
	}

	@media (max-width: 900px) {
		.parameter-grid {
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}
	}

	@media (max-width: 560px) {
		.parameter-grid {
			grid-template-columns: 1fr;
		}
	}
</style>
