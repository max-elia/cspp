export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

export type TrackedStageKey = 'first_stage' | 'scenario_evaluation' | 'reoptimization';
export type StageStatusMap = Partial<Record<TrackedStageKey, string>>;

export type RunsIndex = {
	schema_version?: number;
	updated_at?: string;
	latest_run_id?: string | null;
	runs?: RunIndexEntry[];
};

export type RunIndexEntry = {
	run_id: string;
	instance_id?: string | null;
	label?: string;
	latest?: boolean;
	run_last_modified_at?: string | null;
	status?: string | null;
	last_stage_recorded?: string | null;
	clustering_method?: string | null;
	max_distance_from_warehouse_km?: number | null;
	runtime_id?: string | null;
	runtime_label?: string | null;
	runtime_kind?: string | null;
	started_at?: string | null;
	finished_at?: string | null;
	stage_status?: StageStatusMap;
	frontend_manifest_available?: boolean;
};

export type InstanceCatalogEntry = {
	instance_id: string;
	label?: string;
	latest?: boolean;
	created_at?: string | null;
	updated_at?: string | null;
	source_instance_id?: string | null;
	clustering_method?: string | null;
	max_distance_from_warehouse_km?: number | null;
	customer_count?: number | null;
	demand_row_count?: number | null;
	latest_run_id?: string | null;
	run_count?: number | null;
	runs?: RunIndexEntry[];
};

export type WebAppCatalog = {
	schema_version?: number;
	updated_at?: string;
	latest_instance_id?: string | null;
	latest_run_id?: string | null;
	instances?: InstanceCatalogEntry[];
};

export type FrontendManifest = {
	run_id?: string;
	instance_id?: string;
	generated_at?: string;
	latest_stage?: string | null;
	available_routes?: Record<string, unknown>;
	counts?: Record<string, number>;
	files?: Record<string, string>;
};

export type PipelineEvent = {
	timestamp?: string;
	stage?: string;
	source?: string;
	entity_type?: string;
	entity_id?: string | number | null;
	event?: string | null;
	kind?: string | null;
	phase?: string | null;
	status?: string | null;
	iteration?: number | null;
	message?: string | null;
	metrics?: Record<string, unknown>;
};

export type PipelineAlert = {
	severity?: string;
	stage?: string;
	source?: string;
	entity_type?: string;
	entity_id?: string | number | null;
	message?: string;
	code?: string;
};

export type PipelineEntityState = {
	stage?: string;
	entity_type?: string;
	entity_id?: string | number;
	status?: string;
	phase?: string | null;
	iteration?: number | null;
	started_at?: string | null;
	updated_at?: string | null;
	finished_at?: string | null;
	runtime_sec?: number | null;
	progress?: Record<string, unknown>;
	metrics?: Record<string, unknown>;
	message?: string | null;
	error?: string | null;
	raw?: Record<string, unknown>;
};

export type PipelineStageState = {
	stage_key?: string;
	title?: string;
	status?: string;
	current_phase?: string | null;
	current_iteration?: number | null;
	summary?: Record<string, unknown>;
	entities?: PipelineEntityState[];
	active_entities?: PipelineEntityState[];
	recent_events?: PipelineEvent[];
};

export type PipelineProgress = {
	schema_version?: number;
	run_id?: string;
	updated_at?: string;
	estimate?: PipelineRuntimeEstimate;
	job?: {
		status?: string;
		started_at?: string | null;
		queued_at?: string | null;
		finished_at?: string | null;
		current_stage_key?: string | null;
		current_stage_label?: string | null;
		current_step_key?: string | null;
		current_step_label?: string | null;
		error?: string | null;
		pid?: number | null;
		steps?: string[];
		runtime_id?: string | null;
		runtime_label?: string | null;
		runtime_kind?: string | null;
		queue_position?: number | null;
		runtime_system_cores?: number | null;
		runtime_usable_cores?: number | null;
		last_sync_at?: string | null;
		sync_status?: string | null;
	};
	summary?: Record<string, unknown>;
	stages?: Record<string, PipelineStageState>;
	active_entities?: PipelineEntityState[];
	recent_events?: PipelineEvent[];
	alerts?: PipelineAlert[];
	log_tail?: string[];
};

export type RuntimeProbe = {
	status?: string;
	ready?: boolean;
	checked_at?: string | null;
	error?: string | null;
	system_cores?: number | null;
	usable_cores?: number | null;
};

export type RuntimeQueueInfo = {
	runtime_id?: string;
	active_run_id?: string | null;
	queue_depth?: number;
	queued_run_ids?: string[];
	updated_at?: string | null;
};

export type RuntimeInfo = {
	id: string;
	label?: string;
	kind?: string;
	poll_interval_sec?: number;
	tags?: string[];
	probe?: RuntimeProbe;
	queue?: RuntimeQueueInfo;
};

export type PipelineStageRuntimeEstimate = {
	stage_key?: string;
	label?: string;
	parallel_slots?: number;
	runs_total?: number;
	runs_completed?: number | null;
	estimated_total_sec?: number | null;
	estimated_remaining_sec?: number | null;
	observed_avg_run_sec?: number | null;
	fallback_avg_run_sec?: number | null;
	source?: string;
};

export type PipelineRuntimeEstimate = {
	system_cores?: number;
	usable_cores?: number;
	total_runs?: number;
	estimated_total_sec?: number | null;
	estimated_remaining_sec?: number | null;
	stages?: Record<string, PipelineStageRuntimeEstimate>;
};

export type OverviewPayload = {
	run?: Record<string, unknown>;
	instance?: Record<string, unknown>;
	stage_status?: StageStatusMap;
	stage_cards?: Record<string, unknown>;
	pipeline?: PipelineProgress;
	instance_setup?: Record<string, unknown>;
	recent_events?: Record<string, unknown>[];
	alerts?: AlertItem[];
	map_summary?: Record<string, unknown>;
	summary?: Record<string, unknown>;
};

export type AlertItem = {
	severity?: string;
	stage?: string;
	source?: string;
	message?: string;
};

export type FeatureCollection = {
	type: 'FeatureCollection';
	features: Array<{
		type: 'Feature';
		geometry: { type: 'Point'; coordinates: [number, number] };
		properties: Record<string, unknown>;
	}>;
};

export type MapDemandRow = {
	delivery_date: string;
	client_num: number;
	customer_id?: string | null;
	demand_kg: number;
};

export type InstanceBundleCustomer = {
	client_num: number;
	customer_id?: string | null;
	customer_name?: string | null;
	street?: string | null;
	postal_code?: string | null;
	city?: string | null;
	latitude: number;
	longitude: number;
	total_demand_kg?: number;
	max_demand_kg?: number;
	active_days?: number;
	latest_demand_kg?: number;
	cluster_id?: number | null;
	super_cluster_id?: number | null;
};

export type InstanceBundle = {
	schema_version?: number;
	instance_id?: string | null;
	source_instance_id?: string | null;
	run_id?: string | null;
	source_run_id?: string | null;
	generated_at?: string | null;
	includes_clustering?: boolean | null;
	max_distance_from_warehouse_km?: number | null;
	clustering_method?: string | null;
	warehouse?: {
		latitude?: number | null;
		longitude?: number | null;
	} | null;
	customers: InstanceBundleCustomer[];
	demand_rows: MapDemandRow[];
};

export type PollingState = {
	intervalMs: number;
	inFlight: boolean;
	lastStartedAt: string | null;
	lastCompletedAt: string | null;
	lastSuccessfulAt: string | null;
	consecutiveFailures: number;
	fileCount?: number;
	mode?: string;
};

export type AppState = {
	catalog: WebAppCatalog | null;
	runsIndex: RunsIndex | null;
	selectedInstanceId: string | null;
	selectedRunId: string | null;
	instanceManifest: FrontendManifest | null;
	instanceOverview: OverviewPayload | null;
	manifest: FrontendManifest | null;
	overview: OverviewPayload | null;
	pipelineProgress: PipelineProgress | null;
	instancePayload: Record<string, unknown> | null;
	runConfig: Record<string, unknown> | null;
	mapSummary: Record<string, unknown> | null;
	mapData: FeatureCollection | null;
	mapDemandRows: MapDemandRow[];
	stage1: Record<string, unknown> | null;
	stage2: Record<string, unknown> | null;
	stage3: Record<string, unknown> | null;
	activity: Record<string, unknown> | null;
	alerts: AlertItem[];
	details: {
		stage1Clusters: Record<string, Record<string, unknown>>;
		stage2Scenarios: Record<string, Record<string, unknown>>;
		stage3Clusters: Record<string, Record<string, unknown>>;
		stage3Scopes: Record<string, Record<string, unknown>>;
	};
	polling: PollingState;
	errors: string[];
};
