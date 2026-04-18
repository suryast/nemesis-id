# CloudFront mirror plan for `nemesis.datarakyat.id`

## Target shape

Use one public hostname with same-origin API calls:

- `nemesis.datarakyat.id/*` → static frontend origin
- `nemesis.datarakyat.id/api/*` → Node backend origin

This keeps the browser simple, lets CloudFront cache GET responses, and avoids CORS complexity.

## Recommended origins

### Origin A, frontend
- Static bucket or static host serving `frontend/`
- Cache policy: long TTL for hashed/static assets, short TTL for `index.html`

### Origin B, backend
- Node backend serving the SQLite API
- Forward only `GET`, `HEAD`, `OPTIONS`
- Cache `/api/bootstrap` for 5 minutes
- Cache `/api/regions/*/packages`, `/api/provinces/*/packages`, and `/api/owners/packages` for 60 seconds

## CloudFront behaviors

1. Default behavior: frontend origin
2. Ordered behavior: `/api/*` → backend origin

For `/api/*`:
- Cache based on full query string
- Compress objects
- Respect origin `Cache-Control` and `ETag`

## App expectations

- `frontend/assets/js/runtime-config.js` defaults the API base to `/api`
- Backend now returns:
  - `Cache-Control: public, max-age=300, stale-while-revalidate=3600` on `/api/bootstrap`
  - `Cache-Control: public, max-age=60, stale-while-revalidate=300` on scoped package endpoints
  - weak `ETag` headers for conditional requests

## Notes

- Keep the SQLite backend private behind the backend origin, not directly exposed by a separate public hostname unless needed.
- If `nemesis.datarakyat.id` DNS is managed outside AWS, point it to the CloudFront distribution with the usual CNAME setup.
- If the dataset is updated, backend process restart is enough to clear the in-memory response cache.
