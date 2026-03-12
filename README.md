# Dashboard Lab

## Requisitos

- Node.js 20+
- npm

## Instalacion

```bash
npm install
```

## Configurar Streak

1. Crea `.env` en la raiz del proyecto.
2. Usa este contenido minimo:

```bash
STREAK_API_KEY=tu_clave_streak
API_PORT=4011
VITE_API_PROXY_TARGET=http://localhost:4011
DASHBOARD_HISTORY_START_DAY_KEY=2025-12-15
DASHBOARD_LEGACY_END_DAY_KEY=2026-02-25
DASHBOARD_OFFICIAL_START_DAY_KEY=2026-03-04
DASHBOARD_OFFICIAL_TOTAL_PROVIDERS=1420
DASHBOARD_OFFICIAL_TOTAL_TOLERANCE=1
DASHBOARD_LEGACY_MIN_TOTAL_PROVIDERS=3000
```

3. Reinicia `npm run dev:api` y `npm run dev`.

## Ejecutar en desarrollo

Terminal 1 (API + SQLite):

```bash
npm run dev:api
```

Terminal 2 (frontend):

```bash
npm run dev
```

La API guarda el historial diario en `backend/data/dashboard-history.sqlite`.
Por defecto escucha en `http://localhost:4011` (puedes cambiar con `API_PORT`).
Opcionalmente puedes cambiar `STREAK_API_BASE_URL` (por defecto `https://api.streak.com`).
`DASHBOARD_HISTORY_START_DAY_KEY` define desde que fecha (`YYYY-MM-DD`) se muestra y normaliza el historico.
`DASHBOARD_LEGACY_END_DAY_KEY` y `DASHBOARD_OFFICIAL_START_DAY_KEY` controlan el quiebre legacy/oficial.
`DASHBOARD_OFFICIAL_TOTAL_PROVIDERS` y `DASHBOARD_OFFICIAL_TOTAL_TOLERANCE` controlan el universo objetivo oficial.
`DASHBOARD_LEGACY_MIN_TOTAL_PROVIDERS` define el umbral minimo para clasificar un snapshot como legacy.
Si cambias `API_PORT`, en frontend define `VITE_API_PROXY_TARGET` con el mismo host/puerto antes de `npm run dev`.

Verificacion rapida:

```bash
curl http://localhost:4011/api/streak/status
```

Debe responder `"configured": true`.

## Migrar payloads historicos desde Excel

Cuando ya tienes snapshots guardados en `daily_history_snapshots`, puedes poblar automaticamente el
"dashboard completo por fecha" leyendo tus archivos de Streak en `Downloads`.

```bash
npm run migrate:payloads -- --dir "C:\Users\ignac\Downloads"
```

Opciones utiles:

- `--dry-run`: simula sin guardar.
- `--day YYYY-MM-DD`: migra solo una fecha.
- `--db <ruta>`: usa otra base SQLite.

## Despliegue (Netlify + Render)

### 1) Backend en Render

Este repo incluye [`render.yaml`](./render.yaml) para crear el servicio API.

Variables clave en Render:

- `STREAK_API_KEY` (obligatoria)
- `DASHBOARD_DB_PATH=/var/data/dashboard-history.sqlite` (persistencia)
- `DASHBOARD_OFFICIAL_TOTAL_PROVIDERS=1420`
- `DASHBOARD_LEGACY_END_DAY_KEY=2026-02-25`
- `DASHBOARD_OFFICIAL_START_DAY_KEY=2026-03-04`

Notas:

- El backend ahora usa `PORT` automaticamente (requerido por Render).
- Se recomienda mantener disco persistente (`/var/data`) para no perder historial.

### 2) Frontend en Netlify

Este repo incluye [`netlify.toml`](./netlify.toml) con build/publish.

En Netlify configura esta variable:

- `VITE_API_BASE_URL=https://TU-SERVICIO.onrender.com`

Luego deploy del sitio (build command `npm run build`, publish `dist`).

### 3) Verificacion cruzada

1. Prueba API: `https://TU-SERVICIO.onrender.com/api/health`
2. En el sitio Netlify, confirma que carga snapshots y pipelines sin errores CORS.
3. Si cambia URL de Render, actualiza `VITE_API_BASE_URL` en Netlify y re-deploy.
