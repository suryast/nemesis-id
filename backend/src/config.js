const path = require("path");
const dotenv = require("dotenv");

dotenv.config();

const ROOT_DIR = path.resolve(__dirname, "..");

function resolveFromRoot(value, fallback) {
  const target = value || fallback;
  return path.isAbsolute(target) ? target : path.join(ROOT_DIR, target);
}

const port = Number(process.env.PORT || 3000);

if (!Number.isInteger(port) || port <= 0) {
  throw new Error("PORT must be a positive integer.");
}

const DATA_DIR = resolveFromRoot(process.env.DATA_DIR, "data");
const DATASET_DIR = resolveFromRoot(process.env.AUDIT_DATASET_DIR, "dataset");
const GEO_ROOT_PATH = resolveFromRoot(process.env.GEO_ROOT_PATH, path.join("seed", "geo"));
const bootstrapCacheTtlMs = Number(process.env.BOOTSTRAP_CACHE_TTL_MS || 5 * 60 * 1000);
const scopedPackagesCacheTtlMs = Number(process.env.SCOPED_PACKAGES_CACHE_TTL_MS || 60 * 1000);
const maxScopedResultWindow = Number(process.env.MAX_SCOPED_RESULT_WINDOW || 5000);

if (!Number.isFinite(bootstrapCacheTtlMs) || bootstrapCacheTtlMs < 0) {
  throw new Error("BOOTSTRAP_CACHE_TTL_MS must be zero or a positive number.");
}

if (!Number.isFinite(scopedPackagesCacheTtlMs) || scopedPackagesCacheTtlMs < 0) {
  throw new Error("SCOPED_PACKAGES_CACHE_TTL_MS must be zero or a positive number.");
}

if (!Number.isFinite(maxScopedResultWindow) || maxScopedResultWindow < 1) {
  throw new Error("MAX_SCOPED_RESULT_WINDOW must be a positive number.");
}

module.exports = {
  ROOT_DIR,
  DATA_DIR,
  DATASET_DIR,
  GEO_ROOT_PATH,
  PORT: port,
  CORS_ORIGIN: process.env.CORS_ORIGIN || "*",
  DB_PATH: resolveFromRoot(process.env.SQLITE_PATH, path.join("data", "dashboard.sqlite")),
  GEOJSON_PATH: resolveFromRoot(process.env.GEOJSON_PATH, path.join(GEO_ROOT_PATH, "03-districts")),
  PROVINCE_GEOJSON_PATH: resolveFromRoot(
    process.env.PROVINCE_GEOJSON_PATH,
    path.join(GEO_ROOT_PATH, "02-provinces", "province-only")
  ),
  AUDIT_DATASET_DIR: DATASET_DIR,
  AUDIT_DATASET_YEAR: String(process.env.AUDIT_DATASET_YEAR || "2026").trim(),
  DEFAULT_REGION_PAGE_SIZE: 25,
  MAX_REGION_PAGE_SIZE: 100,
  BOOTSTRAP_CACHE_TTL_MS: bootstrapCacheTtlMs,
  SCOPED_PACKAGES_CACHE_TTL_MS: scopedPackagesCacheTtlMs,
  MAX_SCOPED_RESULT_WINDOW: Math.floor(maxScopedResultWindow),
};
