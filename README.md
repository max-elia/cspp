# Charging Station Placement Planning

This repository contains a self-hostable planning tool for deciding where retail
delivery networks should install electric truck charging stations. It combines a
web dashboard with a command-line pipeline so planners can upload historic
delivery data, set their own cost and vehicle parameters, run the optimization,
and inspect the resulting charger placement and delivery routes.

Thesis PDF: [`thesis.pdf`](thesis.pdf)

The tool is built around a three-stage robust planning workflow:

1. **Cluster solve**: split stores into manageable groups and solve first-stage
   charger decisions on each cluster.
2. **Scenario evaluation**: evaluate the combined charger placement on historic
   demand scenarios.
3. **Cluster reoptimization**: improve cluster-level charger decisions against
   the evaluated upper-bound costs.

The repository intentionally excludes private raw delivery data, thesis sources, full-instance evaluation experiments, and alternative reoptimization method variants.

## Planning Workflow

The intended workflow mirrors a retailer's planning task:

1. **Create an instance** from a single JSON file containing the warehouse,
   stores, and historic demand observations.
2. **Filter and cluster stores** before solving. The web app provides a map
   preview so different clustering methods and group sizes can be compared.
3. **Choose planning parameters**, including vehicle type, charger costs,
   electricity price, waiting cost, truck fixed cost, scenario count, and the
   compute runtime.
4. **Run the solver** locally or on a configured SSH runtime. The dashboard
   tracks stage progress, elapsed time, runtime estimates, and live optimization
   charts.
5. **Review results** on an interactive map with selected charger locations,
   scenario routes, and per-cluster runtime details.

## Requirements

- Python 3.11 or newer
- Gurobi with a valid local license
- Node.js 20 or newer
- pnpm 9 or newer

Set `GRB_LICENSE_FILE` if Gurobi does not find your license automatically:

```bash
export GRB_LICENSE_FILE=/path/to/gurobi.lic
```

Install Python dependencies:

```bash
python3 -m venv .venv
source init_env.sh
pip install -r requirements.txt
```

Install web-app dependencies:

```bash
cd web-app
pnpm install
```

## Instance Data

The supported public input format is a single JSON instance payload. It contains
the depot location, delivery locations, and one demand row per store and delivery
day. Cluster assignments are optional; if they are omitted, the app can create
clusters during setup.

Required top-level fields:

- `schema_version`: current schema version, set to `1`.
- `warehouse`: depot location with `latitude` and `longitude`.
- `stores`: delivery locations. Each store needs a positive integer
  `client_num`, `latitude`, and `longitude`.
- `demand_rows`: historic demand observations with `delivery_date`,
  `client_num`, and non-negative `demand_kg`.

Useful optional store fields include `store_id`, `store_name`, and address
fields, which make the map views easier to inspect but are not required by the
solver. For robust planning, include demand days that represent operationally
difficult patterns, not only an average week: high total demand, many active
stores, broad geographic spread, or known peak conditions are useful scenarios.

Format references and examples:

- [`docs/instance-format.md`](docs/instance-format.md)
- [`schemas/instance-payload.schema.json`](schemas/instance-payload.schema.json)
- [`sample-data/demo/instance_payload.json`](sample-data/demo/instance_payload.json)
- [`thesis.pdf`](thesis.pdf)

Import the bundled sample:

```bash
source init_env.sh
python3 src/run.py import-instance sample-data/demo/instance_payload.json --run-name sample
```

## Run From CLI

List available public stages:

```bash
source init_env.sh
python3 src/run.py list
```

Run the full pipeline on an imported instance:

```bash
python3 src/run.py run all full --run-name sample
```

Or import and solve in one command:

```bash
python3 src/run.py run all full \
  --instance-payload sample-data/demo/instance_payload.json \
  --run-name sample
```

Useful flags:

- `--clustering-method {geographic,angular_slices,angular_slices_store_count,tour_containment}`
- `--vehicle-type {mercedes,volvo}`
- `--scenarios-to-use <n>`
- `--num-customers <n>` for small smoke runs
- `--second-stage-eval-timelimit <seconds>`
- `--second-stage-eval-mipgap <gap>`
- `--reopt-eval-mipgap <gap>`
- `--debug`

Outputs are written to `exports/runs/<run-name>/`.

## Run The Web App

The web app is the main operational interface. It guides users through instance
creation, clustering preview, parameter selection, solve progress, and result
review. It uses a FastAPI backend plus a SvelteKit frontend.

Start the backend:

```bash
source init_env.sh
cd src
uvicorn webserver:app --reload
```

Start the frontend in another terminal:

```bash
cd web-app
pnpm dev
```

Open `http://localhost:5173`. The frontend talks to the backend at `http://127.0.0.1:8000` by default. Override it with `PUBLIC_API_BASE_URL` if needed.

## Web App Runtime Delegation

Optimization runs can be computationally heavy, so the web app can dispatch
several experiments to different compute targets. Configure those targets in
`configs/cspp_runtimes.json`.

- `local` runtimes execute from this checkout and write results under `var/webserver/exports`.
- `ssh` runtimes sync a prepared run to a remote project folder, start the pipeline there, poll status, and sync finished artifacts back.
- Each runtime has its own queue in `var/webserver/state/runtime_queues`, so one run can be active locally while another run is active on a VM.
- Start the backend with `source init_env.sh && cd src && uvicorn webserver:app --reload`; the in-process poller advances all runtime queues.
- Store SSH passwords or key paths in `.env` and reference them from `configs/cspp_runtimes.json` with `password_env` or `ssh_key_path_env`.
- Use the web UI runtime selector when creating a run, or call `POST /api/runs` with `runtime_id=<id>`.

An SSH runtime entry has this shape:

```json
{
  "id": "vm01",
  "label": "VM 01",
  "kind": "ssh",
  "project_root": ".",
  "export_root": "var/webserver/exports",
  "activation_command": "source init_env.sh",
  "prepare_commands": [
    "if [ ! -f .venv/bin/activate ]; then ~/anaconda3/bin/python -m venv --system-site-packages .venv; fi"
  ],
  "poll_interval_sec": 60,
  "source_sync_path": "src/",
  "host": "10.0.0.10",
  "user": "planner",
  "password_env": "CSPP_RUNTIME_VM01_PASSWORD",
  "remote_project_root": "~/max",
  "remote_export_root": "~/max/exports",
  "tags": ["vm"]
}
```

## Screenshots

Create a new instance and inspect alternative clustering results before running
the solver:

![Web app new instance clustering](docs/images/webapp-new-instance-clustering.png)

Track a running solve with stage progress, runtime estimates, and live
optimization charts:

![Web app solver progress](docs/images/webapp-solver-progress-running.png)

## Repository Layout

- `src/run.py`: public CLI entrypoint.
- `src/instance_payload.py`: instance-payload validation/import.
- `src/cspp/`: three-stage CSPP solver implementation.
- `src/clustering/`: public clustering methods.
- `src/webserver.py` and `src/webserver_backend/`: FastAPI backend.
- `web-app/`: SvelteKit frontend.
- `sample-data/`: demo sample instance.
- `docs/` and `schemas/`: input format documentation.

## Notes

The demo data is generated and intended for smoke tests and UI exploration. Full
optimization runs require a working Gurobi installation and can take substantial
time depending on instance size, scenario count, clustering choice, and hardware.
