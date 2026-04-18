const express = require("express");
const cors = require("cors");
const { BOOTSTRAP_CACHE_TTL_MS, CORS_ORIGIN, SCOPED_PACKAGES_CACHE_TTL_MS } = require("./config");
const { getBootstrapPayload, getOwnerPackages, getRegionPackages, getProvincePackages } = require("./dashboard-repository");
const { createResponseCache } = require("./response-cache");
const { writeJsonResponse } = require("./http-cache");

const bootstrapCache = createResponseCache(BOOTSTRAP_CACHE_TTL_MS);
const scopedPackagesCache = createResponseCache(SCOPED_PACKAGES_CACHE_TTL_MS);
const BOOTSTRAP_CACHE_CONTROL = "public, max-age=300, stale-while-revalidate=3600";
const SCOPED_PACKAGES_CACHE_CONTROL = "public, max-age=60, stale-while-revalidate=300";

function resolveCorsOrigin() {
  if (CORS_ORIGIN === "*") {
    return "*";
  }

  return CORS_ORIGIN.split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function createApp(db) {
  const app = express();

  app.use(
    cors({
      origin: resolveCorsOrigin(),
    })
  );
  app.use(express.json());

  app.get("/api/health", (_req, res) => {
    res.json({ status: "ok" });
  });

  app.get("/api/bootstrap", (req, res) => {
    const cacheKey = req.originalUrl;
    const cached = bootstrapCache.get(cacheKey);

    if (cached) {
      writeJsonResponse(req, res, cached, BOOTSTRAP_CACHE_CONTROL);
      return;
    }

    const entry = bootstrapCache.set(cacheKey, getBootstrapPayload(db));
    writeJsonResponse(req, res, entry, BOOTSTRAP_CACHE_CONTROL);
  });

  app.get("/api/regions/:regionKey/packages", (req, res) => {
    const cacheKey = req.originalUrl;
    const cached = scopedPackagesCache.get(cacheKey);

    if (cached) {
      writeJsonResponse(req, res, cached, SCOPED_PACKAGES_CACHE_CONTROL);
      return;
    }

    const payload = getRegionPackages(db, req.params.regionKey, req.query);

    if (!payload) {
      res.status(404).json({ error: "Region not found" });
      return;
    }

    const entry = scopedPackagesCache.set(cacheKey, payload);
    writeJsonResponse(req, res, entry, SCOPED_PACKAGES_CACHE_CONTROL);
  });

  app.get("/api/provinces/:provinceKey/packages", (req, res) => {
    const cacheKey = req.originalUrl;
    const cached = scopedPackagesCache.get(cacheKey);

    if (cached) {
      writeJsonResponse(req, res, cached, SCOPED_PACKAGES_CACHE_CONTROL);
      return;
    }

    const payload = getProvincePackages(db, req.params.provinceKey, req.query);

    if (!payload) {
      res.status(404).json({ error: "Province not found" });
      return;
    }

    const entry = scopedPackagesCache.set(cacheKey, payload);
    writeJsonResponse(req, res, entry, SCOPED_PACKAGES_CACHE_CONTROL);
  });

  app.get("/api/owners/packages", (req, res) => {
    const ownerType = (req.query.ownerType || "").trim();
    const ownerName = (req.query.ownerName || "").trim();

    if (!ownerType || !ownerName) {
      res.status(400).json({ error: "ownerType and ownerName are required" });
      return;
    }

    const cacheKey = req.originalUrl;
    const cached = scopedPackagesCache.get(cacheKey);

    if (cached) {
      writeJsonResponse(req, res, cached, SCOPED_PACKAGES_CACHE_CONTROL);
      return;
    }

    const payload = getOwnerPackages(db, req.query);

    if (!payload) {
      res.status(404).json({ error: "Owner not found" });
      return;
    }

    const entry = scopedPackagesCache.set(cacheKey, payload);
    writeJsonResponse(req, res, entry, SCOPED_PACKAGES_CACHE_CONTROL);
  });

  app.use((err, _req, res, _next) => {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  });

  return app;
}

module.exports = {
  createApp,
};
