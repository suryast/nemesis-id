const VALID_OWNER_TYPES = new Set(["kabkota", "provinsi", "central", "other"])
const VALID_SEVERITIES = new Set(["low", "med", "high", "absurd"])
const LEGEND_COLORS = ["#7b86a3", "#b5a882", "#d4a999", "#8b7332", "#a83c2e"]
const DEFAULT_REGION_PAGE_SIZE = 25
const MAX_REGION_PAGE_SIZE = 100
const MAX_SCOPED_RESULT_WINDOW = 5000
const BOOTSTRAP_CACHE_CONTROL = "public, max-age=300, stale-while-revalidate=3600"
const SCOPED_CACHE_CONTROL = "public, max-age=60, stale-while-revalidate=300"

function jsonResponse(data, init = {}) {
  const headers = new Headers(init.headers || {})
  headers.set("content-type", "application/json; charset=utf-8")
  headers.set("x-content-type-options", "nosniff")
  return new Response(JSON.stringify(data), { ...init, headers })
}

function errorResponse(message, status = 500) {
  return jsonResponse({ error: message }, { status, headers: { "cache-control": "no-store" } })
}

function clampInteger(value, defaultValue, minimum, maximum) {
  const parsed = Number.parseInt(String(value ?? ""), 10)
  if (!Number.isFinite(parsed)) return defaultValue
  return Math.min(Math.max(parsed, minimum), maximum)
}

function parseBooleanQuery(value) {
  if (value === undefined || value === null || value === "") return false
  const normalized = String(value).trim().toLowerCase()
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "ya"
}

function escapeLikePattern(value) {
  return String(value).replace(/[\\%_]/g, (match) => `\\${match}`)
}

function dominantOwnerType(row) {
  const counts = [
    { key: "central", value: row.central_packages || 0 },
    { key: "provinsi", value: row.provincial_packages || 0 },
    { key: "kabkota", value: row.local_packages || 0 },
    { key: "other", value: row.other_packages || 0 },
  ].sort((left, right) => right.value - left.value)

  return counts[0].value > 0 ? counts[0].key : null
}

function buildOwnerMetrics(row) {
  return {
    central: {
      totalPackages: row.central_packages || 0,
      totalPriorityPackages: row.central_priority_packages || 0,
      totalPotentialWaste: row.central_potential_waste || 0,
      totalBudget: row.central_budget || 0,
    },
    provinsi: {
      totalPackages: row.provincial_packages || 0,
      totalPriorityPackages: row.provincial_priority_packages || 0,
      totalPotentialWaste: row.provincial_potential_waste || 0,
      totalBudget: row.provincial_budget || 0,
    },
    kabkota: {
      totalPackages: row.local_packages || 0,
      totalPriorityPackages: row.local_priority_packages || 0,
      totalPotentialWaste: row.local_potential_waste || 0,
      totalBudget: row.local_budget || 0,
    },
    other: {
      totalPackages: row.other_packages || 0,
      totalPriorityPackages: row.other_priority_packages || 0,
      totalPotentialWaste: row.other_potential_waste || 0,
      totalBudget: row.other_budget || 0,
    },
  }
}

function buildProvinceOwnerMetrics(row) {
  return {
    central: { totalPackages: 0, totalPriorityPackages: 0, totalPotentialWaste: 0, totalBudget: 0 },
    provinsi: {
      totalPackages: row.total_packages || 0,
      totalPriorityPackages: row.total_priority_packages || 0,
      totalPotentialWaste: row.total_potential_waste || 0,
      totalBudget: row.total_budget || 0,
    },
    kabkota: { totalPackages: 0, totalPriorityPackages: 0, totalPotentialWaste: 0, totalBudget: 0 },
    other: { totalPackages: 0, totalPriorityPackages: 0, totalPotentialWaste: 0, totalBudget: 0 },
  }
}

function mapOwnerRow(row) {
  return {
    ownerType: row.owner_type,
    ownerName: row.owner_name,
    totalPackages: row.total_packages,
    totalPriorityPackages: row.total_priority_packages,
    totalFlaggedPackages: row.total_flagged_packages,
    totalPotentialWaste: row.total_potential_waste,
    totalBudget: row.total_budget,
    severityCounts: {
      med: row.med_severity_packages,
      high: row.high_severity_packages,
      absurd: row.absurd_severity_packages,
    },
  }
}

function mapRegionRow(row) {
  return {
    regionKey: row.region_key,
    code: row.code,
    provinceName: row.province_name,
    regionName: row.region_name,
    regionType: row.region_type,
    displayName: row.display_name,
    totalPackages: row.total_packages,
    totalPriorityPackages: row.total_priority_packages,
    totalFlaggedPackages: row.total_flagged_packages,
    totalPotentialWaste: row.total_potential_waste,
    totalBudget: row.total_budget,
    avgRiskScore: Number(Number(row.avg_risk_score || 0).toFixed(2)),
    maxRiskScore: row.max_risk_score,
    ownerMix: {
      central: row.central_packages,
      provinsi: row.provincial_packages,
      kabkota: row.local_packages,
      other: row.other_packages,
    },
    ownerMetrics: buildOwnerMetrics(row),
    severityCounts: {
      med: row.med_severity_packages,
      high: row.high_severity_packages,
      absurd: row.absurd_severity_packages,
    },
    dominantOwnerType: dominantOwnerType(row),
  }
}

function mapProvinceRow(row) {
  return {
    provinceKey: row.province_key,
    code: row.code,
    provinceName: row.province_name,
    regionName: row.province_name,
    regionType: "Provinsi",
    displayName: row.display_name,
    totalPackages: row.total_packages,
    totalPriorityPackages: row.total_priority_packages,
    totalFlaggedPackages: row.total_flagged_packages,
    totalPotentialWaste: row.total_potential_waste,
    totalBudget: row.total_budget,
    avgRiskScore: Number(Number(row.avg_risk_score || 0).toFixed(2)),
    maxRiskScore: row.max_risk_score,
    ownerMix: {
      central: 0,
      provinsi: row.total_packages,
      kabkota: 0,
      other: 0,
    },
    ownerMetrics: buildProvinceOwnerMetrics(row),
    severityCounts: {
      med: row.med_severity_packages,
      high: row.high_severity_packages,
      absurd: row.absurd_severity_packages,
    },
    dominantOwnerType: row.total_packages > 0 ? "provinsi" : null,
  }
}

function mapPackageRow(row) {
  return {
    id: row.id,
    sourceId: row.source_id,
    packageName: row.package_name,
    ownerName: row.owner_name,
    ownerType: row.owner_type,
    satker: row.satker,
    locationRaw: row.location_raw,
    budget: row.budget,
    fundingSource: row.funding_source,
    procurementType: row.procurement_type,
    procurementMethod: row.procurement_method,
    selectionDate: row.selection_date,
    audit: {
      schemaVersion: row.schema_version,
      severity: row.severity,
      potensiPemborosan: row.potential_waste,
      reason: row.reason,
      flags: {
        isMencurigakan: row.is_mencurigakan === null ? null : Boolean(row.is_mencurigakan),
        isPemborosan: row.is_pemborosan === null ? null : Boolean(row.is_pemborosan),
      },
    },
    meta: {
      isPriority: Boolean(row.is_priority),
      isFlagged: Boolean(row.is_flagged),
      riskScore: row.risk_score,
      activeTagCount: row.active_tag_count,
      mappedRegionCount: row.mapped_region_count,
    },
  }
}

function buildLegend(values) {
  const positiveValues = values.filter((value) => value > 0).sort((left, right) => left - right)
  const ranges = []

  if (!positiveValues.length) {
    return { zeroColor: "#243155", ranges }
  }

  const quantiles = [0.2, 0.4, 0.6, 0.8, 1].map((ratio) => {
    const index = Math.min(positiveValues.length - 1, Math.floor((positiveValues.length - 1) * ratio))
    return positiveValues[index]
  })

  let minimum = positiveValues[0]
  for (let index = 0; index < quantiles.length; index += 1) {
    const maximum = quantiles[index]
    if (maximum < minimum) continue
    if (ranges.length && maximum === ranges[ranges.length - 1].max) continue
    ranges.push({ key: `band-${index + 1}`, color: LEGEND_COLORS[Math.min(index, LEGEND_COLORS.length - 1)], min: minimum, max: maximum })
    minimum = maximum + 0.01
  }

  return { zeroColor: "#243155", ranges }
}

function normalizeScopedPackageQuery(url, options = {}) {
  const page = clampInteger(url.searchParams.get("page"), 1, 1, Number.MAX_SAFE_INTEGER)
  const pageSize = clampInteger(url.searchParams.get("pageSize"), DEFAULT_REGION_PAGE_SIZE, 1, MAX_REGION_PAGE_SIZE)
  const offset = (page - 1) * pageSize

  if (offset >= MAX_SCOPED_RESULT_WINDOW) {
    const error = new Error(`Requested page exceeds max result window of ${MAX_SCOPED_RESULT_WINDOW} rows.`)
    error.statusCode = 400
    throw error
  }

  return {
    page,
    pageSize,
    offset,
    search: (url.searchParams.get("search") || "").trim(),
    ownerType: options.allowOwnerType === false ? "" : (url.searchParams.get("ownerType") || "").trim(),
    severity: options.allowSeverity === false ? "" : (url.searchParams.get("severity") || "").trim(),
    priorityOnly: parseBooleanQuery(url.searchParams.get("priorityOnly")),
  }
}

function resolveRegionTotalItems(row, query) {
  if (query.search || query.severity) return null

  if (VALID_OWNER_TYPES.has(query.ownerType)) {
    const packageFieldByOwnerType = {
      central: "central_packages",
      provinsi: "provincial_packages",
      kabkota: "local_packages",
      other: "other_packages",
    }
    const priorityFieldByOwnerType = {
      central: "central_priority_packages",
      provinsi: "provincial_priority_packages",
      kabkota: "local_priority_packages",
      other: "other_priority_packages",
    }
    const fieldName = query.priorityOnly ? priorityFieldByOwnerType[query.ownerType] : packageFieldByOwnerType[query.ownerType]
    return row[fieldName] || 0
  }

  return query.priorityOnly ? row.total_priority_packages || 0 : row.total_packages || 0
}

function resolveProvinceTotalItems(row, query) {
  if (query.search || query.severity) return null
  return query.priorityOnly ? row.total_priority_packages || 0 : row.total_packages || 0
}

function resolveOwnerTotalItems(row, query) {
  if (query.search || query.severity) return null
  return query.priorityOnly ? row.total_priority_packages || 0 : row.total_packages || 0
}

function buildPackagesWhereClause(scopeColumn, scopeKey, query, options = {}) {
  const clauses = [`${scopeColumn} = ?`]
  const params = [scopeKey]

  if (query.search) {
    const searchValue = `%${escapeLikePattern(query.search)}%`
    clauses.push("(packages.package_name LIKE ? ESCAPE '\\\\' OR packages.owner_name LIKE ? ESCAPE '\\\\' OR COALESCE(packages.satker, '') LIKE ? ESCAPE '\\\\')")
    params.push(searchValue, searchValue, searchValue)
  }

  if (options.forcedOwnerType) {
    clauses.push("packages.owner_type = ?")
    params.push(options.forcedOwnerType)
  } else if (VALID_OWNER_TYPES.has(query.ownerType)) {
    clauses.push("packages.owner_type = ?")
    params.push(query.ownerType)
  }

  if (options.allowSeverity !== false && VALID_SEVERITIES.has(query.severity)) {
    clauses.push("packages.severity = ?")
    params.push(query.severity)
  }

  if (query.priorityOnly) {
    clauses.push("packages.is_priority = 1")
  }

  return { sql: clauses.join(" AND "), params }
}

function buildOwnerPackagesWhereClause(ownerType, ownerName, query) {
  const clauses = ["packages.owner_type = ?", "packages.owner_name = ?"]
  const params = [ownerType, ownerName]

  if (query.search) {
    const searchValue = `%${escapeLikePattern(query.search)}%`
    clauses.push("(packages.package_name LIKE ? ESCAPE '\\\\' OR packages.owner_name LIKE ? ESCAPE '\\\\' OR COALESCE(packages.satker, '') LIKE ? ESCAPE '\\\\')")
    params.push(searchValue, searchValue, searchValue)
  }

  if (VALID_SEVERITIES.has(query.severity)) {
    clauses.push("packages.severity = ?")
    params.push(query.severity)
  }

  if (query.priorityOnly) {
    clauses.push("packages.is_priority = 1")
  }

  return { sql: clauses.join(" AND "), params }
}

async function all(db, sql, params = []) {
  const result = await db.prepare(sql).bind(...params).all()
  return result.results || []
}

async function first(db, sql, params = []) {
  return db.prepare(sql).bind(...params).first()
}

async function queryPackagesPage(db, scopeTable, scopeColumn, scopeKey, normalizedQuery, options = {}) {
  const whereClause = buildPackagesWhereClause(scopeColumn, scopeKey, normalizedQuery, options)
  const totalItems = Number.isInteger(options.precomputedTotalItems)
    ? options.precomputedTotalItems
    : ((await first(
        db,
        `SELECT COUNT(*) AS total FROM ${scopeTable} INNER JOIN packages ON packages.id = ${scopeTable}.package_id WHERE ${whereClause.sql}`,
        whereClause.params,
      ))?.total || 0)
  const totalPages = totalItems ? Math.ceil(totalItems / normalizedQuery.pageSize) : 1
  const page = Math.min(normalizedQuery.page, totalPages)
  const offset = (page - 1) * normalizedQuery.pageSize
  const rows = await all(
    db,
    `SELECT
      packages.id,
      packages.source_id,
      packages.schema_version,
      packages.owner_name,
      packages.owner_type,
      packages.satker,
      packages.package_name,
      packages.location_raw,
      packages.budget,
      packages.funding_source,
      packages.procurement_type,
      packages.procurement_method,
      packages.selection_date,
      packages.potential_waste,
      packages.severity,
      packages.reason,
      packages.is_mencurigakan,
      packages.is_pemborosan,
      packages.risk_score,
      packages.active_tag_count,
      packages.is_priority,
      packages.is_flagged,
      packages.mapped_region_count
    FROM ${scopeTable}
    INNER JOIN packages ON packages.id = ${scopeTable}.package_id
    WHERE ${whereClause.sql}
    ORDER BY
      packages.is_priority DESC,
      packages.potential_waste DESC,
      packages.risk_score DESC,
      COALESCE(packages.budget, 0) DESC,
      packages.inserted_order ASC
    LIMIT ? OFFSET ?`,
    [...whereClause.params, normalizedQuery.pageSize, offset],
  )

  return { totalItems, page, pageSize: normalizedQuery.pageSize, totalPages, rows: rows.map(mapPackageRow) }
}

async function queryOwnerPackagesPage(db, ownerType, ownerName, normalizedQuery, options = {}) {
  const whereClause = buildOwnerPackagesWhereClause(ownerType, ownerName, normalizedQuery)
  const totalItems = Number.isInteger(options.precomputedTotalItems)
    ? options.precomputedTotalItems
    : ((await first(db, `SELECT COUNT(*) AS total FROM packages WHERE ${whereClause.sql}`, whereClause.params))?.total || 0)
  const totalPages = totalItems ? Math.ceil(totalItems / normalizedQuery.pageSize) : 1
  const page = Math.min(normalizedQuery.page, totalPages)
  const offset = (page - 1) * normalizedQuery.pageSize
  const rows = await all(
    db,
    `SELECT
      packages.id,
      packages.source_id,
      packages.schema_version,
      packages.owner_name,
      packages.owner_type,
      packages.satker,
      packages.package_name,
      packages.location_raw,
      packages.budget,
      packages.funding_source,
      packages.procurement_type,
      packages.procurement_method,
      packages.selection_date,
      packages.potential_waste,
      packages.severity,
      packages.reason,
      packages.is_mencurigakan,
      packages.is_pemborosan,
      packages.risk_score,
      packages.active_tag_count,
      packages.is_priority,
      packages.is_flagged,
      packages.mapped_region_count
    FROM packages
    WHERE ${whereClause.sql}
    ORDER BY
      packages.is_priority DESC,
      packages.potential_waste DESC,
      packages.risk_score DESC,
      COALESCE(packages.budget, 0) DESC,
      packages.inserted_order ASC
    LIMIT ? OFFSET ?`,
    [...whereClause.params, normalizedQuery.pageSize, offset],
  )
  return { totalItems, page, pageSize: normalizedQuery.pageSize, totalPages, rows: rows.map(mapPackageRow) }
}

async function getBootstrapPayload(db) {
  const summaryRow = await first(db, `
    SELECT
      COUNT(*) AS total_packages,
      COALESCE(SUM(is_priority), 0) AS total_priority_packages,
      COALESCE(ROUND(SUM(potential_waste), 2), 0) AS total_potential_waste,
      COALESCE(SUM(COALESCE(budget, 0)), 0) AS total_budget,
      COALESCE(SUM(CASE WHEN mapped_region_count = 0 THEN 1 ELSE 0 END), 0) AS unmapped_packages,
      COALESCE(SUM(CASE WHEN mapped_region_count > 1 THEN 1 ELSE 0 END), 0) AS multi_location_packages
    FROM packages
  `)

  const regionRows = await all(db, `
    SELECT
      regions.region_key,
      regions.code,
      regions.province_name,
      regions.region_name,
      regions.region_type,
      regions.display_name,
      region_metrics.total_packages,
      region_metrics.total_priority_packages,
      region_metrics.total_flagged_packages,
      region_metrics.total_potential_waste,
      region_metrics.total_budget,
      region_metrics.avg_risk_score,
      region_metrics.max_risk_score,
      region_metrics.central_packages,
      region_metrics.provincial_packages,
      region_metrics.local_packages,
      region_metrics.other_packages,
      region_metrics.central_priority_packages,
      region_metrics.provincial_priority_packages,
      region_metrics.local_priority_packages,
      region_metrics.other_priority_packages,
      region_metrics.central_potential_waste,
      region_metrics.provincial_potential_waste,
      region_metrics.local_potential_waste,
      region_metrics.other_potential_waste,
      region_metrics.central_budget,
      region_metrics.provincial_budget,
      region_metrics.local_budget,
      region_metrics.other_budget,
      region_metrics.med_severity_packages,
      region_metrics.high_severity_packages,
      region_metrics.absurd_severity_packages
    FROM regions
    INNER JOIN region_metrics ON region_metrics.region_key = regions.region_key
    ORDER BY
      region_metrics.total_potential_waste DESC,
      region_metrics.total_priority_packages DESC,
      region_metrics.total_packages DESC,
      regions.display_name ASC
  `)

  const provinceRows = await all(db, `
    SELECT
      provinces.province_key,
      provinces.code,
      provinces.province_name,
      provinces.display_name,
      province_metrics.total_packages,
      province_metrics.total_priority_packages,
      province_metrics.total_flagged_packages,
      province_metrics.total_potential_waste,
      province_metrics.total_budget,
      province_metrics.avg_risk_score,
      province_metrics.max_risk_score,
      province_metrics.med_severity_packages,
      province_metrics.high_severity_packages,
      province_metrics.absurd_severity_packages
    FROM provinces
    INNER JOIN province_metrics ON province_metrics.province_key = provinces.province_key
    ORDER BY
      province_metrics.total_potential_waste DESC,
      province_metrics.total_priority_packages DESC,
      province_metrics.total_packages DESC,
      provinces.display_name ASC
  `)

  const ownerRows = await all(db, `
    SELECT
      owner_metrics.owner_type,
      owner_metrics.owner_name,
      owner_metrics.total_packages,
      owner_metrics.total_priority_packages,
      owner_metrics.total_flagged_packages,
      owner_metrics.total_potential_waste,
      owner_metrics.total_budget,
      owner_metrics.med_severity_packages,
      owner_metrics.high_severity_packages,
      owner_metrics.absurd_severity_packages
    FROM owner_metrics
    WHERE owner_metrics.owner_type = ?
    ORDER BY
      owner_metrics.total_potential_waste DESC,
      owner_metrics.total_priority_packages DESC,
      owner_metrics.total_packages DESC,
      owner_metrics.owner_name ASC
  `, ["central"])

  const regions = regionRows.map(mapRegionRow)
  const provinces = provinceRows.map(mapProvinceRow)

  return {
    summary: {
      totalPackages: summaryRow?.total_packages || 0,
      totalPriorityPackages: summaryRow?.total_priority_packages || 0,
      totalPotentialWaste: summaryRow?.total_potential_waste || 0,
      totalBudget: summaryRow?.total_budget || 0,
      unmappedPackages: summaryRow?.unmapped_packages || 0,
      multiLocationPackages: summaryRow?.multi_location_packages || 0,
    },
    legend: buildLegend(regions.map((region) => region.totalPotentialWaste)),
    geoUrl: "/assets/data/audit-geojson.json",
    regions,
    provinceView: {
      legend: buildLegend(provinces.map((province) => province.totalPotentialWaste)),
      geoUrl: "/assets/data/audit-province-geojson.json",
      provinces,
    },
    ownerLists: {
      central: ownerRows.map(mapOwnerRow),
    },
  }
}

async function getRegionPackages(db, regionKey, url) {
  const regionRow = await first(db, `
    SELECT
      regions.region_key,
      regions.code,
      regions.province_name,
      regions.region_name,
      regions.region_type,
      regions.display_name,
      region_metrics.total_packages,
      region_metrics.total_priority_packages,
      region_metrics.total_flagged_packages,
      region_metrics.total_potential_waste,
      region_metrics.total_budget,
      region_metrics.avg_risk_score,
      region_metrics.max_risk_score,
      region_metrics.central_packages,
      region_metrics.provincial_packages,
      region_metrics.local_packages,
      region_metrics.other_packages,
      region_metrics.central_priority_packages,
      region_metrics.provincial_priority_packages,
      region_metrics.local_priority_packages,
      region_metrics.other_priority_packages,
      region_metrics.central_potential_waste,
      region_metrics.provincial_potential_waste,
      region_metrics.local_potential_waste,
      region_metrics.other_potential_waste,
      region_metrics.central_budget,
      region_metrics.provincial_budget,
      region_metrics.local_budget,
      region_metrics.other_budget,
      region_metrics.med_severity_packages,
      region_metrics.high_severity_packages,
      region_metrics.absurd_severity_packages
    FROM regions
    INNER JOIN region_metrics ON region_metrics.region_key = regions.region_key
    WHERE regions.region_key = ?
  `, [regionKey])

  if (!regionRow) return null
  const normalizedQuery = normalizeScopedPackageQuery(url)
  const pageResult = await queryPackagesPage(db, "package_regions", "package_regions.region_key", regionKey, normalizedQuery, {
    precomputedTotalItems: resolveRegionTotalItems(regionRow, normalizedQuery),
  })
  return {
    region: mapRegionRow(regionRow),
    summary: { totalItems: pageResult.totalItems, filteredItems: pageResult.totalItems },
    pagination: { page: pageResult.page, pageSize: pageResult.pageSize, totalItems: pageResult.totalItems, totalPages: pageResult.totalPages },
    filters: {
      search: normalizedQuery.search,
      ownerType: normalizedQuery.ownerType,
      severity: normalizedQuery.severity,
      priorityOnly: normalizedQuery.priorityOnly,
    },
    items: pageResult.rows,
  }
}

async function getProvincePackages(db, provinceKey, url) {
  const provinceRow = await first(db, `
    SELECT
      provinces.province_key,
      provinces.code,
      provinces.province_name,
      provinces.display_name,
      province_metrics.total_packages,
      province_metrics.total_priority_packages,
      province_metrics.total_flagged_packages,
      province_metrics.total_potential_waste,
      province_metrics.total_budget,
      province_metrics.avg_risk_score,
      province_metrics.max_risk_score,
      province_metrics.med_severity_packages,
      province_metrics.high_severity_packages,
      province_metrics.absurd_severity_packages
    FROM provinces
    INNER JOIN province_metrics ON province_metrics.province_key = provinces.province_key
    WHERE provinces.province_key = ?
  `, [provinceKey])

  if (!provinceRow) return null
  const normalizedQuery = normalizeScopedPackageQuery(url, { allowOwnerType: false })
  const pageResult = await queryPackagesPage(db, "package_provinces", "package_provinces.province_key", provinceKey, normalizedQuery, {
    forcedOwnerType: "provinsi",
    precomputedTotalItems: resolveProvinceTotalItems(provinceRow, normalizedQuery),
  })
  return {
    province: mapProvinceRow(provinceRow),
    summary: { totalItems: pageResult.totalItems, filteredItems: pageResult.totalItems },
    pagination: { page: pageResult.page, pageSize: pageResult.pageSize, totalItems: pageResult.totalItems, totalPages: pageResult.totalPages },
    filters: {
      search: normalizedQuery.search,
      severity: normalizedQuery.severity,
      priorityOnly: normalizedQuery.priorityOnly,
    },
    items: pageResult.rows,
  }
}

async function getOwnerPackages(db, url) {
  const ownerType = (url.searchParams.get("ownerType") || "").trim()
  const ownerName = (url.searchParams.get("ownerName") || "").trim()
  if (!VALID_OWNER_TYPES.has(ownerType) || !ownerName) return null

  const ownerRow = await first(db, `
    SELECT
      owner_metrics.owner_type,
      owner_metrics.owner_name,
      owner_metrics.total_packages,
      owner_metrics.total_priority_packages,
      owner_metrics.total_flagged_packages,
      owner_metrics.total_potential_waste,
      owner_metrics.total_budget,
      owner_metrics.med_severity_packages,
      owner_metrics.high_severity_packages,
      owner_metrics.absurd_severity_packages
    FROM owner_metrics
    WHERE owner_metrics.owner_type = ?
      AND owner_metrics.owner_name = ?
  `, [ownerType, ownerName])

  if (!ownerRow) return null
  const normalizedQuery = normalizeScopedPackageQuery(url, { allowOwnerType: false })
  const pageResult = await queryOwnerPackagesPage(db, ownerType, ownerName, normalizedQuery, {
    precomputedTotalItems: resolveOwnerTotalItems(ownerRow, normalizedQuery),
  })
  return {
    owner: mapOwnerRow(ownerRow),
    summary: { totalItems: pageResult.totalItems, filteredItems: pageResult.totalItems },
    pagination: { page: pageResult.page, pageSize: pageResult.pageSize, totalItems: pageResult.totalItems, totalPages: pageResult.totalPages },
    filters: {
      search: normalizedQuery.search,
      severity: normalizedQuery.severity,
      priorityOnly: normalizedQuery.priorityOnly,
    },
    items: pageResult.rows,
  }
}

async function withCache(request, cacheControl, ctx, producer) {
  const cache = caches.default
  const cached = await cache.match(request)
  if (cached) return cached
  const response = await producer()
  if (response.ok && request.method === "GET") {
    const cacheable = new Response(response.body, response)
    cacheable.headers.set("cache-control", cacheControl)
    ctx.waitUntil(cache.put(request, cacheable.clone()))
    return cacheable
  }
  return response
}

async function handleApi(request, env, ctx) {
  const url = new URL(request.url)
  if (url.pathname === "/api/health") {
    return jsonResponse({ status: "ok", service: "nemesis-id", backend: "cloudflare-d1" }, { headers: { "cache-control": "no-store" } })
  }

  if (!env.DB) {
    return errorResponse("D1 binding DB is missing", 500)
  }

  if (url.pathname === "/api/bootstrap") {
    return withCache(request, BOOTSTRAP_CACHE_CONTROL, ctx, async () => {
      const payload = await getBootstrapPayload(env.DB)
      return jsonResponse(payload, { headers: { "cache-control": BOOTSTRAP_CACHE_CONTROL } })
    })
  }

  const regionMatch = url.pathname.match(/^\/api\/regions\/([^/]+)\/packages$/)
  if (regionMatch) {
    return withCache(request, SCOPED_CACHE_CONTROL, ctx, async () => {
      const payload = await getRegionPackages(env.DB, decodeURIComponent(regionMatch[1]), url)
      return payload
        ? jsonResponse(payload, { headers: { "cache-control": SCOPED_CACHE_CONTROL } })
        : errorResponse("Region not found", 404)
    })
  }

  const provinceMatch = url.pathname.match(/^\/api\/provinces\/([^/]+)\/packages$/)
  if (provinceMatch) {
    return withCache(request, SCOPED_CACHE_CONTROL, ctx, async () => {
      const payload = await getProvincePackages(env.DB, decodeURIComponent(provinceMatch[1]), url)
      return payload
        ? jsonResponse(payload, { headers: { "cache-control": SCOPED_CACHE_CONTROL } })
        : errorResponse("Province not found", 404)
    })
  }

  if (url.pathname === "/api/owners/packages") {
    return withCache(request, SCOPED_CACHE_CONTROL, ctx, async () => {
      const payload = await getOwnerPackages(env.DB, url)
      return payload
        ? jsonResponse(payload, { headers: { "cache-control": SCOPED_CACHE_CONTROL } })
        : errorResponse("Owner not found", 404)
    })
  }

  return null
}

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url)
      if (url.pathname.startsWith("/api/")) {
        const apiResponse = await handleApi(request, env, ctx)
        if (apiResponse) return apiResponse
        return errorResponse("Not found", 404)
      }

      if (env.ASSETS && typeof env.ASSETS.fetch === "function") {
        return env.ASSETS.fetch(request)
      }

      return new Response("Static assets binding missing", { status: 500 })
    } catch (error) {
      console.error("[nemesis-worker]", error)
      return errorResponse(error?.statusCode ? error.message : "Internal server error", error?.statusCode || 500)
    }
  },
}
