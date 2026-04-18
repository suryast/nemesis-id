function shouldSendNotModified(req, etag) {
  const requestEtag = req.headers["if-none-match"];

  if (!requestEtag || !etag) {
    return false;
  }

  return requestEtag
    .split(",")
    .map((value) => value.trim())
    .includes(etag);
}

function writeJsonResponse(req, res, entry, cacheControl) {
  res.set("Content-Type", "application/json; charset=utf-8");
  res.set("Cache-Control", cacheControl);
  res.set("ETag", entry.etag);

  if (shouldSendNotModified(req, entry.etag)) {
    res.status(304).end();
    return;
  }

  res.send(entry.body);
}

module.exports = {
  writeJsonResponse,
};
