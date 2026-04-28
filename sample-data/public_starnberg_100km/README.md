# Public Starnberg 100 km Sample

Synthetic CSPP sample instance for GitHub demos. Store names, identifiers, addresses, coordinates, and demand rows are generated data. The warehouse is centered near Starnberg, Germany, and stores are randomly placed within a 100 km radius. The demand profile only preserves broad scale and sparsity characteristics needed to exercise the web app.

- Seed: `20260424`
- Stores: `48`
- Demand rows: `1488`
- Files: `instance_payload.json`, `assignments.json`, `customers.geojson`

The `clustering_method` is set to `angular_slices` for pipeline compatibility. The included cluster IDs are synthetic angular sectors around the Starnberg warehouse.
