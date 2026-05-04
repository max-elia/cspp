# Instance Payload Format

The public pipeline accepts a single JSON file. The file is imported into the internal solver tables by:

```bash
python3 src/run.py import-instance path/to/instance_payload.json --run-name my-run
```

The full machine-readable schema is in [`schemas/instance-payload.schema.json`](../schemas/instance-payload.schema.json).

## Top-Level Object

```json
{
  "schema_version": 1,
  "instance_id": "my-instance",
  "warehouse": {
    "latitude": 47.999,
    "longitude": 11.339
  },
  "stores": [],
  "demand_rows": []
}
```

Required fields:

- `schema_version`: currently `1`.
- `warehouse`: depot location. Latitude and longitude are required.
- `stores`: non-empty list of delivery locations.
- `demand_rows`: demand observations for those locations.

Optional top-level fields:

- `instance_id`: stable identifier used in generated run metadata.

Unknown fields are preserved in the stored payload but ignored by the solver.

## Warehouse

```json
{
  "latitude": 47.999,
  "longitude": 11.339
}
```

Coordinates must be WGS84 decimal degrees.

## Stores

Each store entry represents one store or delivery location.

```json
{
  "client_num": 1,
  "store_id": "STORE-001",
  "store_name": "Sample Store 001",
  "street": "Main Street 1",
  "postal_code": "80331",
  "city": "Munich",
  "latitude": 48.137154,
  "longitude": 11.576124,
  "cluster_id": 0
}
```

Required fields:

- `client_num`: positive integer, unique across the instance. This is the solver node id.
- `latitude`, `longitude`: WGS84 decimal degrees.

Recommended fields:

- `store_id`: external id shown in exports and the web app.
- `store_name`, `street`, `postal_code`, `city`: display metadata.
- `cluster_id`: non-negative integer cluster assignment. Either provide this for every store or omit it for every store. Mixed clustered and unclustered store lists are rejected.

If `cluster_id` is omitted, no clustering information is required in the payload. The web app can create cluster assignments in its setup step, and the CLI can create them by running the clustering stage.

Optional summary fields such as `total_demand_kg`, `max_demand_kg`, and `active_days` are useful for the web app but are not required by the solver.

## Demand Rows

Demand is long-form: one row per store and delivery date.

```json
{
  "delivery_date": "2026-01-15",
  "client_num": 1,
  "store_id": "STORE-001",
  "demand_kg": 1250.5
}
```

Required fields:

- `delivery_date`: non-empty string. ISO dates (`YYYY-MM-DD`) are recommended.
- `client_num`: must reference an existing store.
- `demand_kg`: non-negative numeric demand.

`store_id` is optional and is used only for display/cross-reference.

## Importer Output

The importer writes a run directory under `exports/runs/<run-name>/` and derives:

- `prep/instance/payload.json`: original payload.
- `02_generate_instance_data/data/coordinates.json`: warehouse plus store coordinates.
- `02_generate_instance_data/data/demand_matrix.json`: wide demand matrix by delivery date.
- `02_generate_instance_data/data/distances_matrix.json`: haversine distance matrix in kilometers.
- `prep/clustering/assignments.json`: only when all stores include `cluster_id`.

The pipeline can then solve the imported instance with:

```bash
python3 src/run.py run all full --run-name my-run
```
