const crypto = require("crypto");

function createResponseCache(defaultTtlMs) {
  const store = new Map();

  function get(key) {
    const entry = store.get(key);

    if (!entry) {
      return null;
    }

    if (entry.expiresAt <= Date.now()) {
      store.delete(key);
      return null;
    }

    return entry;
  }

  function set(key, payload, ttlMs = defaultTtlMs) {
    const body = JSON.stringify(payload);
    const entry = {
      body,
      etag: `W/\"${crypto.createHash("sha1").update(body).digest("hex")}\"`,
      expiresAt: Date.now() + ttlMs,
    };

    store.set(key, entry);
    return entry;
  }

  return {
    get,
    set,
  };
}

module.exports = {
  createResponseCache,
};
