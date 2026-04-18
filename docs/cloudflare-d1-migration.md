# Cloudflare D1 migration notes

## Why the current SQLite dump does not fit D1 as-is

The current `assets` table stores very large JSON blobs:

- `audit_geojson`: ~25.6 MB
- `audit_province_geojson`: ~18.2 MB

D1 row size is much smaller, so these blobs should not live in D1.

## Recommended split

- **D1**: relational tables and queryable metrics
  - `packages`
  - `regions`
  - `provinces`
  - `region_metrics`
  - `province_metrics`
  - `owner_metrics`
  - `package_regions`
  - `package_provinces`
- **Static assets**:
  - `frontend/assets/data/audit-geojson.json`
  - `frontend/assets/data/audit-province-geojson.json`

## Runtime shape

- `wrangler.toml` serves frontend assets directly from the Worker assets binding
- Worker handles `/api/*`
- Frontend loads the large geojson files lazily from static asset URLs returned by `/api/bootstrap`

## Prep import file

```bash
python3 scripts/make-d1-import.py   --in /path/to/dashboard.sql   --out /path/to/d1-import.sql
```

## Create database

```bash
wrangler d1 create nemesis-id
```

Put the returned database id into `wrangler.toml`.

## Import schema/data

```bash
wrangler d1 execute nemesis-id --file /path/to/d1-import.sql
```

## Post-import optimization

```bash
wrangler d1 execute nemesis-id --command "ANALYZE;"
```

## Deploy

```bash
wrangler deploy
```

Then route the Worker to `nemesis.datarakyat.id`.
