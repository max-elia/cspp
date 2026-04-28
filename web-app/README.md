# CSPP Web App

SvelteKit frontend for browsing imported instances, starting CSPP runs, and inspecting the three pipeline stages.

## Development

Install dependencies:

```bash
pnpm install
```

Start the FastAPI backend from the repository root:

```bash
source init_env.sh
cd src
uvicorn webserver:app --reload
```

Start the frontend:

```bash
pnpm dev
```

The frontend defaults to `http://127.0.0.1:8000` for API calls. Override it with:

```bash
PUBLIC_API_BASE_URL=http://127.0.0.1:8000 pnpm dev
```

## Build

```bash
pnpm build
```

Preview the built app:

```bash
pnpm preview
```
