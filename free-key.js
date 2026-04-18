const crypto = require("crypto");
const fs = require("fs");
const https = require("https");
const path = require("path");

module.exports = function registerFreeKeyRoutes(app, deps) {
  const FREE_ROUTE =
    String(process.env.FREE_ROUTE || "free").replace(/^\/+|\/+$/g, "") || "free";
  const FREE_STORE_PATH = path.join(deps.rootDir, ".data", "free_claims.json");
  const FREE_GITHUB_DATA_PATH =
    process.env.FREE_GITHUB_DATA_PATH || ".data/free_claims.json";
  const FREE_KEYS_STORE_PATH = path.join(deps.rootDir, ".data", "free_keys.json");
  const FREE_KEYS_GITHUB_DATA_PATH =
    process.env.FREE_KEYS_GITHUB_DATA_PATH || ".data/free_keys.json";
  const FREE_STATS_ROUTE = String(process.env.FREE_STATS_ROUTE || "")
    .trim()
    .replace(/^\/+|\/+$/g, "");
  const FREE_REPO_CONFIG = {
    token:
      process.env.FREE_GITHUB_TOKEN ||
      (deps.freeStoreConfig && deps.freeStoreConfig.token) ||
      "",
    repo:
      process.env.FREE_GITHUB_REPO ||
      (deps.freeStoreConfig && deps.freeStoreConfig.repo) ||
      "",
    branch:
      process.env.FREE_GITHUB_BRANCH ||
      (deps.freeStoreConfig && deps.freeStoreConfig.branch) ||
      "main"
  };
  const FREE_LINK4M_URL_TEMPLATE = String(
    process.env.FREE_LINK4M_URL_TEMPLATE || ""
  ).trim();
  const FREE_LINK4M_API_TOKEN = String(
    process.env.FREE_LINK4M_API_TOKEN || process.env.LINK4M_API_TOKEN || ""
  ).trim();
  const FREE_LINK4M_API_ENDPOINT = String(
    process.env.FREE_LINK4M_API_ENDPOINT || "https://link4m.co/api-shorten/v2"
  ).trim();
  const FREE_TURNSTILE_SITE_KEY = String(
    process.env.FREE_TURNSTILE_SITE_KEY || process.env.TURNSTILE_SITE_KEY || ""
  ).trim();
  const FREE_TURNSTILE_SECRET_KEY = String(
    process.env.FREE_TURNSTILE_SECRET_KEY || process.env.TURNSTILE_SECRET_KEY || ""
  ).trim();
  const FREE_KEY_DAYS = Math.max(0.0416667, Number(process.env.FREE_KEY_DAYS || 1));
  const FREE_KEY_USES = Math.max(1, Number(process.env.FREE_KEY_USES || 1));
  const FREE_CLAIM_TTL_MS = Math.max(
    60 * 1000,
    Number(process.env.FREE_CLAIM_TTL_MS || 10 * 60 * 1000)
  );
  const FREE_COOLDOWN_MS = Math.max(
    60 * 1000,
    Number(process.env.FREE_COOLDOWN_MS || 24 * 60 * 60 * 1000)
  );
  const FREE_MIN_ELAPSED_MS = Math.max(
    5 * 1000,
    Number(process.env.FREE_MIN_ELAPSED_MS || 15 * 1000)
  );
  const FREE_REFERER_KEYWORD = String(
    process.env.FREE_REFERER_KEYWORD || "link4m"
  )
    .trim()
    .toLowerCase();
  const FREE_STRICT_IP_MATCH =
    String(process.env.FREE_STRICT_IP_MATCH || "true").trim().toLowerCase() !==
    "false";
  const FREE_LOG_PATH = path.join(deps.rootDir, ".data", "free_key.log");
  const FREE_LOG_MAX_BYTES = Math.max(
    64 * 1024,
    Number(process.env.FREE_LOG_MAX_BYTES || 512 * 1024)
  );
  const FREE_CLAIM_HISTORY_MS = Math.max(
    5 * 60 * 1000,
    Number(process.env.FREE_CLAIM_HISTORY_MS || 2 * 60 * 60 * 1000)
  );
  const FREE_FINISHED_CLAIM_HISTORY_MS = Math.max(
    5 * 60 * 1000,
    Math.min(FREE_CLAIM_HISTORY_MS, 20 * 60 * 1000)
  );
  const FREE_PENDING_CLAIM_GRACE_MS = 10 * 60 * 1000;
  const FREE_RATE_BUCKET_TTL_MS = Math.max(
    5 * 60 * 1000,
    Number(process.env.FREE_RATE_BUCKET_TTL_MS || 20 * 60 * 1000)
  );
  const FREE_RATE_BUCKET_MAX_KEYS = Math.max(
    200,
    Number(process.env.FREE_RATE_BUCKET_MAX_KEYS || 3000)
  );
  const FREE_HOUSEKEEPING_INTERVAL_MS = Math.max(
    5 * 60 * 1000,
    Number(process.env.FREE_HOUSEKEEPING_INTERVAL_MS || 10 * 60 * 1000)
  );

  let freeStore = { claims: {}, cooldowns: {} };
  let freeKeys = {};
  let freeReady = false;
  let freeSaveQueue = Promise.resolve();
  let freeKeysSaveQueue = Promise.resolve();
  let lastSyncedFreeKeys = {};
  let freeHousekeepingTimer = null;
  const freeRateMap = new Map();
  const STAT_EVENT_MAP = {
    claim_started: "started",
    claim_verified: "verified",
    claim_issued: "issued",
    cooldown_hit: "cooldownHit",
    callback_too_fast: "tooFast",
    callback_bad_referer: "badReferer",
    callback_invalid_state: "invalidState",
    callback_cookie_mismatch: "cookieMismatch",
    callback_browser_mismatch: "browserMismatch",
    callback_ip_mismatch: "ipMismatch",
    callback_ua_mismatch: "uaMismatch",
    callback_expired: "expired",
    callback_missing_claim: "missingClaim",
    link4m_create_failed: "linkCreateFailed",
    turnstile_failed: "captchaFailed",
    turnstile_missing: "captchaMissing",
    turnstile_error: "captchaError",
    rate_limit: "rateLimit"
  };

  function esc(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function hashValue(value) {
    return crypto.createHash("sha256").update(String(value || "")).digest("hex");
  }

  function signValue(value) {
    return crypto
      .createHmac("sha256", deps.sessionSecret)
      .update("free|" + value)
      .digest("hex");
  }

  function createToken(payload) {
    const encoded = Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
    return encoded + "." + signValue(encoded);
  }

  function verifyToken(token) {
    try {
      const raw = String(token || "");
      const dotIndex = raw.lastIndexOf(".");
      if (dotIndex <= 0) return null;
      const encoded = raw.slice(0, dotIndex);
      const sig = raw.slice(dotIndex + 1);
      if (sig !== signValue(encoded)) return null;
      return JSON.parse(Buffer.from(encoded, "base64url").toString("utf8"));
    } catch {
      return null;
    }
  }

  function ensureLocalDir(filePath) {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
  }

  function appendLogLine(line) {
    try {
      fs.mkdirSync(path.dirname(FREE_LOG_PATH), { recursive: true });
      fs.appendFileSync(FREE_LOG_PATH, `${line}\n`, "utf8");
      trimFreeLogIfNeeded();
    } catch (err) {
      console.error("Write free log failed:", err.message);
    }
  }

  function trimFreeLogIfNeeded() {
    try {
      if (!fs.existsSync(FREE_LOG_PATH)) return;
      const stat = fs.statSync(FREE_LOG_PATH);
      if (!stat || stat.size <= FREE_LOG_MAX_BYTES) return;

      const keepBytes = Math.max(32 * 1024, Math.floor(FREE_LOG_MAX_BYTES * 0.7));
      const raw = fs.readFileSync(FREE_LOG_PATH, "utf8");
      if (Buffer.byteLength(raw, "utf8") <= FREE_LOG_MAX_BYTES) return;

      let trimmed = raw.slice(-keepBytes);
      const firstNewline = trimmed.indexOf("\n");
      if (firstNewline >= 0) {
        trimmed = trimmed.slice(firstNewline + 1);
      }
      trimmed = trimmed.trim();
      fs.writeFileSync(FREE_LOG_PATH, trimmed ? `${trimmed}\n` : "", "utf8");
    } catch (err) {
      console.error("Trim free log failed:", err.message);
    }
  }

  function logEvent(type, req, details = {}) {
    const entry = {
      time: new Date().toISOString(),
      type,
      ipHash: hashValue(getClientIp(req)),
      uaHash: hashValue(getUserAgent(req)),
      path: req.path,
      ...details
    };
    const line = JSON.stringify(entry);
    console.log("[free-key]", line);
    appendLogLine(line);
  }

  function normalizeKeyStore(store) {
    if (typeof deps.normalizeAllStore === "function") {
      return deps.normalizeAllStore(store || {});
    }
    return store && typeof store === "object" ? { ...store } : {};
  }

  function cloneJson(value) {
    return JSON.parse(JSON.stringify(value || {}));
  }

  function applyExternalKeyDeletes(snapshot, latestStore, lastSyncedStore) {
    const next = cloneJson(snapshot);
    Object.keys(lastSyncedStore || {}).forEach((key) => {
      if (!latestStore[key] && next[key]) {
        delete next[key];
      }
    });
    return next;
  }

  function hasFreeGithubStore() {
    return typeof deps.hasGithubStore === "function"
      ? deps.hasGithubStore({ ...FREE_REPO_CONFIG, dataPath: FREE_GITHUB_DATA_PATH })
      : false;
  }

  function readFreeGithubStore(dataPath) {
    return deps.readGithubStore(dataPath, FREE_REPO_CONFIG);
  }

  function writeFreeGithubStore(snapshot, dataPath, commitMessage) {
    return deps.writeGithubStore(snapshot, dataPath, commitMessage, FREE_REPO_CONFIG);
  }

  function createEmptyDayStats() {
    return {
      started: 0,
      verified: 0,
      verifiedDevices: 0,
      issued: 0,
      issuedDevices: 0,
      cooldownHit: 0,
      tooFast: 0,
      badReferer: 0,
      invalidState: 0,
      cookieMismatch: 0,
      browserMismatch: 0,
      ipMismatch: 0,
      uaMismatch: 0,
      expired: 0,
      missingClaim: 0,
      linkCreateFailed: 0,
      captchaFailed: 0,
      captchaMissing: 0,
      captchaError: 0,
      rateLimit: 0
    };
  }

  function normalizeDayStats(day) {
    const base = createEmptyDayStats();
    Object.keys(base).forEach((key) => {
      base[key] = Math.max(0, Number(day && day[key] ? day[key] : 0));
    });
    return base;
  }

  function getDayKey(value = Date.now()) {
    return new Intl.DateTimeFormat("en-CA", {
      timeZone: deps.vnTimezone || "Asia/Ho_Chi_Minh",
      year: "numeric",
      month: "2-digit",
      day: "2-digit"
    }).format(new Date(value));
  }

  function loadJsonFile(filePath, fallbackValue) {
    try {
      ensureLocalDir(filePath);
      if (!fs.existsSync(filePath)) return fallbackValue;
      const raw = fs.readFileSync(filePath, "utf8");
      return JSON.parse(raw || "null") ?? fallbackValue;
    } catch {
      return fallbackValue;
    }
  }

  function writeJsonFile(filePath, value) {
    ensureLocalDir(filePath);
    fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
  }

  function normalizeFreeStore(store) {
    const out = { claims: {}, cooldowns: {} };
    if (store && typeof store === "object") {
      if (store.claims && typeof store.claims === "object") {
        Object.keys(store.claims).forEach((claimId) => {
          const claim = store.claims[claimId];
          if (!claim || typeof claim !== "object") return;
          out.claims[claimId] = {
            id: String(claim.id || claimId),
            createdAt: Number(claim.createdAt || 0),
            expiresAt: Number(claim.expiresAt || 0),
            browserHash: String(claim.browserHash || ""),
            uaHash: String(claim.uaHash || ""),
            ipHash: String(claim.ipHash || ""),
            identityHash: String(claim.identityHash || ""),
            status: String(claim.status || "started"),
            verifiedAt: Number(claim.verifiedAt || 0),
            claimedAt: Number(claim.claimedAt || 0),
            key: String(claim.key || ""),
            keyExpireAt: Number(claim.keyExpireAt || 0)
          };
        });
      }
      if (store.cooldowns && typeof store.cooldowns === "object") {
        Object.keys(store.cooldowns).forEach((identityHash) => {
          const until = Number(store.cooldowns[identityHash] || 0);
          if (until > 0) out.cooldowns[identityHash] = until;
        });
      }
    }
    return out;
  }

  function cleanupFreeStore() {
    const now = Date.now();
    let changed = false;
    const keepClaims = {};
    Object.keys(freeStore.claims || {}).forEach((claimId) => {
      const claim = freeStore.claims[claimId];
      if (!claim || typeof claim !== "object") {
        changed = true;
        return;
      }
      const claimedAt = Number(claim.claimedAt || 0);
      const expiresAt = Number(claim.expiresAt || 0);
      if (
        claim.status === "claimed" &&
        claimedAt > 0 &&
        claimedAt + FREE_FINISHED_CLAIM_HISTORY_MS > now
      ) {
        keepClaims[claimId] = claim;
        return;
      }
      if (expiresAt > now || expiresAt + FREE_PENDING_CLAIM_GRACE_MS > now) {
        keepClaims[claimId] = claim;
        return;
      }
      changed = true;
    });

    const keepCooldowns = {};
    Object.keys(freeStore.cooldowns || {}).forEach((identityHash) => {
      const until = Number(freeStore.cooldowns[identityHash] || 0);
      if (until > now) {
        keepCooldowns[identityHash] = until;
        return;
      }
      if (until > 0) changed = true;
    });

    freeStore = { claims: keepClaims, cooldowns: keepCooldowns };
    return changed;
  }

  function cleanupExpiredFreeKeys(options = {}) {
    const now = Date.now();
    const keepKey = String(options.keepKey || "").trim();
    let changed = false;
    const next = {};
    Object.keys(freeKeys || {}).forEach((key) => {
      const item = normalizeKeyStore({ [key]: freeKeys[key] })[key];
      if (!item) return;
      const expireAt = Number(item.expireAt || 0);
      if (expireAt > 0 && expireAt <= now && key !== keepKey) {
        changed = true;
        return;
      }
      next[key] = item;
    });
    if (changed) {
      freeKeys = next;
    } else {
      freeKeys = normalizeKeyStore(freeKeys);
    }
    return changed;
  }

  function loadLocalStore() {
    try {
      return normalizeFreeStore(loadJsonFile(FREE_STORE_PATH, { claims: {}, cooldowns: {} }));
    } catch {
      return { claims: {}, cooldowns: {} };
    }
  }

  function saveLocalStore(snapshot) {
    writeJsonFile(FREE_STORE_PATH, snapshot);
  }

  function loadLocalFreeKeys() {
    return normalizeKeyStore(loadJsonFile(FREE_KEYS_STORE_PATH, {}));
  }

  function saveLocalFreeKeys(snapshot) {
    writeJsonFile(FREE_KEYS_STORE_PATH, snapshot);
  }

  async function initStore() {
    try {
      if (hasFreeGithubStore()) {
        freeStore = normalizeFreeStore(await readFreeGithubStore(FREE_GITHUB_DATA_PATH));
      } else {
        freeStore = loadLocalStore();
      }
    } catch (err) {
      console.error("Free store init failed, fallback local:", err.message);
      freeStore = loadLocalStore();
    }
    return cleanupFreeStore();
  }

  async function initFreeKeysStore() {
    try {
      if (hasFreeGithubStore()) {
        freeKeys = normalizeKeyStore(
          await readFreeGithubStore(FREE_KEYS_GITHUB_DATA_PATH)
        );
        const changed = cleanupExpiredFreeKeys();
        lastSyncedFreeKeys = cloneJson(freeKeys);
        return changed;
      } else {
        freeKeys = loadLocalFreeKeys();
        const changed = cleanupExpiredFreeKeys();
        lastSyncedFreeKeys = cloneJson(freeKeys);
        return changed;
      }
    } catch (err) {
      console.error("Free keys init failed, fallback local:", err.message);
      freeKeys = loadLocalFreeKeys();
      const changed = cleanupExpiredFreeKeys();
      lastSyncedFreeKeys = cloneJson(freeKeys);
      return changed;
    }
  }

  async function persistFreeStore(snapshot) {
    try {
      if (hasFreeGithubStore()) {
        await writeFreeGithubStore(snapshot, FREE_GITHUB_DATA_PATH, "Update free claims store");
      } else {
        saveLocalStore(snapshot);
      }
    } catch (err) {
      console.error("Persist free store failed:", err.message);
      saveLocalStore(snapshot);
    }
  }

  async function persistFreeKeysStore(snapshot) {
    try {
      if (hasFreeGithubStore()) {
        const remoteNow = normalizeKeyStore(
          await readFreeGithubStore(FREE_KEYS_GITHUB_DATA_PATH)
        );
        const nextSnapshot = applyExternalKeyDeletes(
          snapshot,
          remoteNow,
          lastSyncedFreeKeys
        );
        await writeFreeGithubStore(
          nextSnapshot,
          FREE_KEYS_GITHUB_DATA_PATH,
          "Update free keys store"
        );
        lastSyncedFreeKeys = cloneJson(nextSnapshot);
      } else {
        const localNow = loadLocalFreeKeys();
        const nextSnapshot = applyExternalKeyDeletes(
          snapshot,
          localNow,
          lastSyncedFreeKeys
        );
        saveLocalFreeKeys(nextSnapshot);
        lastSyncedFreeKeys = cloneJson(nextSnapshot);
      }
    } catch (err) {
      console.error("Persist free keys failed:", err.message);
      saveLocalFreeKeys(snapshot);
      lastSyncedFreeKeys = cloneJson(snapshot);
    }
  }

  async function saveStore(options = {}) {
    const background = !!options.background;
    cleanupFreeStore();
    const snapshot = JSON.parse(JSON.stringify(freeStore));
    freeSaveQueue = freeSaveQueue
      .then(() => persistFreeStore(snapshot))
      .catch((err) => {
        console.error("Free save queue failed:", err.message);
      });
    if (!background) return freeSaveQueue;
    return Promise.resolve();
  }

  function saveFreeKeys(options = {}) {
    const background = !!options.background;
    cleanupExpiredFreeKeys();
    freeKeys = normalizeKeyStore(freeKeys);
    const snapshot = JSON.parse(JSON.stringify(freeKeys));
    freeKeysSaveQueue = freeKeysSaveQueue
      .then(() => persistFreeKeysStore(snapshot))
      .catch((err) => {
        console.error("Free keys queue failed:", err.message);
      });
    if (background) return Promise.resolve();
    return freeKeysSaveQueue;
  }

  function removeFreeKey(key, options = {}) {
    const normalizedKey = String(key || "").trim();
    if (!normalizedKey || !freeKeys[normalizedKey]) {
      return options.background ? Promise.resolve() : Promise.resolve();
    }
    delete freeKeys[normalizedKey];
    return saveFreeKeys(options);
  }

  function syncFreeHousekeepingInBackground(options = {}) {
    const storeChanged = cleanupFreeStore();
    const keysChanged = cleanupExpiredFreeKeys(options);
    if (storeChanged) {
      saveStore({ background: true }).catch(() => {});
    }
    if (keysChanged) {
      saveFreeKeys({ background: true }).catch(() => {});
    }
    return storeChanged || keysChanged;
  }

  function startFreeHousekeeping() {
    syncFreeHousekeepingInBackground();
    if (freeHousekeepingTimer) return;
    freeHousekeepingTimer = setInterval(() => {
      syncFreeHousekeepingInBackground();
    }, FREE_HOUSEKEEPING_INTERVAL_MS);
    if (typeof freeHousekeepingTimer.unref === "function") {
      freeHousekeepingTimer.unref();
    }
  }

  async function migrateLegacyFreeKeys() {
    if (typeof deps.getKeys !== "function") return;
    const mainKeys = deps.getKeys();
    if (!mainKeys || typeof mainKeys !== "object") return;

    let changedMain = false;
    let changedFree = false;
    Object.keys(mainKeys).forEach((key) => {
      const item =
        typeof deps.normalizeKeyItem === "function"
          ? deps.normalizeKeyItem(mainKeys[key])
          : mainKeys[key];
      if (!item || String(item.source || "") !== "free") return;
      if (!freeKeys[key]) {
        freeKeys[key] = item;
        changedFree = true;
      }
      delete mainKeys[key];
      changedMain = true;
    });

    if (changedFree) await saveFreeKeys();
    if (changedMain && typeof deps.saveKeys === "function") await deps.saveKeys();
  }

  function getClientIp(req) {
    return (
      String(req.headers["x-forwarded-for"] || "")
        .split(",")[0]
        .trim() ||
      req.socket.remoteAddress ||
      "unknown"
    );
  }

  function getUserAgent(req) {
    return String(req.headers["user-agent"] || "");
  }

  function getBaseUrl(req) {
    const proto =
      String(req.headers["x-forwarded-proto"] || req.protocol || "https")
        .split(",")[0]
        .trim() || "https";
    const host =
      String(req.headers["x-forwarded-host"] || req.get("host") || "")
        .split(",")[0]
        .trim();
    return proto + "://" + host;
  }

  function buildLink4mUrl(targetUrl) {
    if (!FREE_LINK4M_URL_TEMPLATE) return "";
    return FREE_LINK4M_URL_TEMPLATE.replace(
      /\{\{\s*target\s*\}\}/gi,
      encodeURIComponent(targetUrl)
    ).replace(/\{\{\s*target_raw\s*\}\}/gi, targetUrl);
  }

  function requestJson(url, options = {}) {
    return new Promise((resolve, reject) => {
      const target = new URL(url);
      const body = options.body || "";
      const method = options.method || (body ? "POST" : "GET");
      const headers = {
        "User-Agent": "aimtrickhead-free-key",
        Accept: "application/json",
        ...(options.headers || {})
      };
      if (body && !headers["Content-Length"]) {
        headers["Content-Length"] = Buffer.byteLength(body);
      }
      const req = https.request(
        {
          protocol: target.protocol,
          hostname: target.hostname,
          port: target.port || undefined,
          path: `${target.pathname}${target.search}`,
          method,
          timeout: 10000,
          headers
        },
        (res) => {
          let raw = "";
          res.on("data", (chunk) => {
            raw += chunk;
          });
          res.on("end", () => {
            try {
              const parsed = JSON.parse(raw || "{}");
              if (res.statusCode >= 200 && res.statusCode < 300) {
                resolve(parsed);
                return;
              }
              reject(
                new Error(
                  parsed.message ||
                    parsed.error ||
                    `Link4m API returned ${res.statusCode || 500}`
                )
              );
            } catch (err) {
              reject(new Error("Link4m API trả về dữ liệu không hợp lệ."));
            }
          });
        }
      );
      req.on("error", reject);
      req.on("timeout", () => req.destroy(new Error("Link4m API timeout.")));
      if (body) req.write(body);
      req.end();
    });
  }

  async function createLink4mUrl(targetUrl) {
    if (FREE_LINK4M_API_TOKEN) {
      const apiUrl = new URL(FREE_LINK4M_API_ENDPOINT);
      apiUrl.searchParams.set("api", FREE_LINK4M_API_TOKEN);
      apiUrl.searchParams.set("url", targetUrl);
      const result = await requestJson(apiUrl.toString());
      const shortenedUrl = String(
        result.shortenedUrl || result.shortened_url || result.url || ""
      ).trim();
      if (
        String(result.status || "").toLowerCase() !== "success" ||
        !shortenedUrl
      ) {
        throw new Error("Không tạo được link Link4m.");
      }
      return shortenedUrl;
    }

    const templatedUrl = buildLink4mUrl(targetUrl);
    if (templatedUrl) return templatedUrl;
    throw new Error("Chua cau hinh Link4m API hoac template.");
  }

  async function verifyTurnstileToken(token, req) {
    if (!FREE_TURNSTILE_SECRET_KEY) return { ok: true, skipped: true };
    const body = new URLSearchParams({
      secret: FREE_TURNSTILE_SECRET_KEY,
      response: token,
      remoteip: getClientIp(req)
    }).toString();

    const result = await requestJson(
      "https://challenges.cloudflare.com/turnstile/v0/siteverify",
      {
        method: "POST",
        body,
        headers: {
          "Content-Type": "application/x-www-form-urlencoded"
        }
      }
    );

    return {
      ok: !!result.success,
      skipped: false,
      codes: Array.isArray(result["error-codes"]) ? result["error-codes"] : []
    };
  }

  function parseCookies(req) {
    const out = {};
    const raw = String(req.headers.cookie || "");
    raw.split(";").forEach((part) => {
      const trimmed = part.trim();
      if (!trimmed) return;
      const eqIndex = trimmed.indexOf("=");
      if (eqIndex <= 0) return;
      const key = trimmed.slice(0, eqIndex).trim();
      const value = trimmed.slice(eqIndex + 1).trim();
      out[key] = decodeURIComponent(value);
    });
    return out;
  }

  function appendSetCookie(res, cookieValue) {
    const current = res.getHeader("Set-Cookie");
    if (!current) {
      res.setHeader("Set-Cookie", cookieValue);
      return;
    }
    if (Array.isArray(current)) {
      res.setHeader("Set-Cookie", current.concat(cookieValue));
      return;
    }
    res.setHeader("Set-Cookie", [current, cookieValue]);
  }

  function setCookie(res, req, name, value, maxAgeMs) {
    const cookieParts = [
      `${name}=${encodeURIComponent(value)}`,
      "Path=/",
      "HttpOnly",
      "SameSite=Lax",
      `Max-Age=${Math.max(0, Math.floor(maxAgeMs / 1000))}`
    ];
    const proto =
      String(req.headers["x-forwarded-proto"] || req.protocol || "")
        .split(",")[0]
        .trim()
        .toLowerCase();
    if (proto === "https" || process.env.NODE_ENV === "production") {
      cookieParts.push("Secure");
    }
    appendSetCookie(res, cookieParts.join("; "));
  }

  function clearCookie(res, req, name) {
    setCookie(res, req, name, "", 0);
  }

  function formatDuration(ms) {
    const totalMinutes = Math.max(1, Math.ceil(ms / 60000));
    if (totalMinutes < 60) return `${totalMinutes} phut`;
    const hours = Math.floor(totalMinutes / 60);
    const minutes = totalMinutes % 60;
    if (!minutes) return `${hours} gio`;
    return `${hours} gio ${minutes} phut`;
  }

  function aggregateStatsFromLog() {
    const out = { days: {} };
    const verifiedDeviceSets = {};
    const issuedDeviceSets = {};
    const claimIdentityMap = {};
    try {
      if (!fs.existsSync(FREE_LOG_PATH)) return out;
      const raw = fs.readFileSync(FREE_LOG_PATH, "utf8");
      raw.split(/\r?\n/).forEach((line) => {
        const trimmed = line.trim();
        if (!trimmed) return;
        try {
          const entry = JSON.parse(trimmed);
          const field = STAT_EVENT_MAP[String(entry.type || "").trim()];
          if (!field) return;
          const dayKey = getDayKey(entry.time || Date.now());
          const day = normalizeDayStats(out.days[dayKey]);
          day[field] += 1;
          const claimId = String(entry.claimId || "").trim();
          const identityHash = String(
            entry.identityHash || (claimId ? claimIdentityMap[claimId] || "" : "")
          ).trim();
          if (claimId && identityHash) {
            claimIdentityMap[claimId] = identityHash;
          }
          if (field === "verified" && identityHash) {
            if (!verifiedDeviceSets[dayKey]) verifiedDeviceSets[dayKey] = new Set();
            verifiedDeviceSets[dayKey].add(identityHash);
            day.verifiedDevices = verifiedDeviceSets[dayKey].size;
          }
          if (field === "issued" && identityHash) {
            if (!issuedDeviceSets[dayKey]) issuedDeviceSets[dayKey] = new Set();
            issuedDeviceSets[dayKey].add(identityHash);
            day.issuedDevices = issuedDeviceSets[dayKey].size;
          }
          out.days[dayKey] = day;
        } catch {}
      });
    } catch (err) {
      console.error("Read free log stats failed:", err.message);
    }
    return out;
  }

  function sumStats(list) {
    return list.reduce((acc, entry) => {
      const normalized = normalizeDayStats(entry);
      Object.keys(acc).forEach((key) => {
        acc[key] += normalized[key];
      });
      return acc;
    }, createEmptyDayStats());
  }

  function getStatsSnapshot() {
    cleanupFreeStore();
    const logStats = aggregateStatsFromLog();
    const todayKey = getDayKey();
    const sortedDays = Object.keys(logStats.days || {}).sort((a, b) =>
      a < b ? 1 : a > b ? -1 : 0
    );
    const last7Days = sortedDays.slice(0, 7).map((day) => ({
      day,
      ...normalizeDayStats(logStats.days[day])
    }));
    const recentIssued = Object.values(freeStore.claims || {})
      .filter((claim) => claim && claim.status === "claimed" && claim.key)
      .sort((a, b) => Number(b.claimedAt || 0) - Number(a.claimedAt || 0))
      .slice(0, 12)
      .map((claim) => ({
        key: claim.key,
        claimedAt: Number(claim.claimedAt || 0),
        claimedText: deps.formatVNTime(Number(claim.claimedAt || 0)),
        expireAt: Number(claim.keyExpireAt || 0),
        expireText: deps.formatVNTime(Number(claim.keyExpireAt || 0))
      }));

    return {
      today: {
        day: todayKey,
        ...normalizeDayStats(logStats.days[todayKey])
      },
      last7Days,
      totals: sumStats(Object.values(logStats.days || {})),
      activeCooldowns: Object.keys(freeStore.cooldowns || {}).length,
      pendingClaims: Object.values(freeStore.claims || {}).filter(
        (claim) => claim && claim.status !== "claimed" && Number(claim.expiresAt || 0) > Date.now()
      ).length,
      freeKeyCount: Object.keys(freeKeys || {}).length,
      statsDays: Object.keys(logStats.days || {}).length,
      recentIssued
    };
  }

  function renderStatsPage() {
    return `
<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">
  <meta name="theme-color" content="#08090c">
  <title>Free Stats</title>
  ${deps.baseStyles()}
  <style>
    .statsCard{max-width:760px}
    .statsGrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin-top:14px}
    .statsMeta{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin-top:14px}
    .statsTable{width:100%;border-collapse:collapse;margin-top:12px;font-size:13px}
    .statsTable th,.statsTable td{padding:10px 8px;text-align:left;border-bottom:1px solid rgba(255,255,255,.07)}
    .statsTable th{color:#e9dec1;font-weight:700}
    .statsLogin{display:flex;gap:10px;align-items:center}
    .statsLogin .input{flex:1}
    .statsCode{font-family:Consolas,monospace;font-size:12px;color:#d6d9ff;overflow-wrap:anywhere}
    @media (max-width:640px){
      .statsGrid,.statsMeta{grid-template-columns:1fr}
      .statsLogin{flex-direction:column}
      .statsLogin .btn{width:100%}
    }
  </style>
</head>
<body>
  <div class="bgAura"><div class="orb o1"></div><div class="orb o2"></div><div class="orb o3"></div></div>
  <div class="wrap">
    <div class="card statsCard">
      <div class="top">
        <div class="brand">
          <div class="logoBox">${deps.renderLogo(78, 24)}</div>
          <div>
            <h1 class="title">Free Stats</h1>
            <div class="sub">Thống kê hệ key free tách riêng</div>
            <div class="credit">Route ẩn theo env, dữ liệu không trộn với admin keys</div>
          </div>
        </div>
      </div>
      <div class="content">
        <div class="tile">
          <div class="sub" style="margin:0 0 10px">Nhập admin key để xem thống kê free.</div>
          <div class="statsLogin">
            <input id="statsAdminKey" class="input" type="password" placeholder="Admin key">
            <button id="statsLoadBtn" class="btn" style="margin-top:0">Tải thống kê</button>
          </div>
          <div id="statsStatus" class="msg" style="margin-top:12px"></div>
        </div>
        <div id="statsRoot" class="hidden">
          <div class="statsMeta">
            <div class="statChip"><span class="statLabel">Key free</span><strong id="freeKeyCount">0</strong></div>
            <div class="statChip"><span class="statLabel">Cooldown active</span><strong id="activeCooldowns">0</strong></div>
            <div class="statChip"><span class="statLabel">Claim pending</span><strong id="pendingClaims">0</strong></div>
          </div>
          <div class="statsGrid">
            <div class="tile"><div class="name">Hôm nay</div><div class="desc" id="todaySummary">-</div></div>
            <div class="tile"><div class="name">Tổng 7 ngày</div><div class="desc" id="totalSummary">-</div></div>
          </div>
          <div class="tile">
            <div class="name">7 ngày gần nhất</div>
            <div style="overflow:auto">
              <table class="statsTable">
                <thead>
                  <tr><th>Ngày</th><th>Start</th><th>Verified</th><th>Issued</th><th>Cooldown</th><th>Fail</th></tr>
                </thead>
                <tbody id="statsDaysBody"></tbody>
              </table>
            </div>
          </div>
          <div class="tile">
            <div class="name">Key free mới cấp</div>
            <div id="recentIssuedBox" class="desc" style="margin-top:12px"></div>
          </div>
        </div>
      </div>
    </div>
  </div>
  <script>
    (function () {
      var status = document.getElementById("statsStatus");
      var root = document.getElementById("statsRoot");
      var btn = document.getElementById("statsLoadBtn");
      var input = document.getElementById("statsAdminKey");
      function toFailCount(day) {
        return (day.tooFast || 0) + (day.badReferer || 0) + (day.invalidState || 0) +
          (day.cookieMismatch || 0) + (day.browserMismatch || 0) + (day.ipMismatch || 0) +
          (day.uaMismatch || 0) + (day.expired || 0) + (day.missingClaim || 0) +
          (day.linkCreateFailed || 0) + (day.captchaFailed || 0) + (day.captchaMissing || 0) +
          (day.captchaError || 0) + (day.rateLimit || 0);
      }
      function textSummary(day) {
        return "Start: " + (day.started || 0) + " | Verified: " + (day.verified || 0) + " | Issued: " + (day.issued || 0) + " | Cooldown: " + (day.cooldownHit || 0) + " | Fail: " + toFailCount(day);
      }
      function render(data) {
        root.classList.remove("hidden");
        document.getElementById("freeKeyCount").textContent = data.freeKeyCount || 0;
        document.getElementById("activeCooldowns").textContent = data.activeCooldowns || 0;
        document.getElementById("pendingClaims").textContent = data.pendingClaims || 0;
        document.getElementById("todaySummary").textContent = textSummary(data.today || {});
        document.getElementById("totalSummary").textContent = textSummary(data.totals || {});
        document.getElementById("statsDaysBody").innerHTML = (data.last7Days || []).map(function (day) {
          return "<tr><td>" + day.day + "</td><td>" + (day.started || 0) + "</td><td>" + (day.verified || 0) + "</td><td>" + (day.issued || 0) + "</td><td>" + (day.cooldownHit || 0) + "</td><td>" + toFailCount(day) + "</td></tr>";
        }).join("") || '<tr><td colspan="6">Chưa có dữ liệu</td></tr>';
        document.getElementById("recentIssuedBox").innerHTML = (data.recentIssued || []).map(function (item) {
          return '<div class="statsCode">' + item.key + ' | cấp: ' + item.claimedText + ' | hạn: ' + item.expireText + '</div>';
        }).join("") || "Chưa có key free nào được cấp.";
      }
      btn.addEventListener("click", async function () {
        var adminKey = (input.value || "").trim();
        if (!adminKey) {
          status.className = "msg err";
          status.textContent = "Nhập admin key trước.";
          return;
        }
        btn.disabled = true;
        status.className = "msg";
        status.textContent = "Đang tải thống kê...";
        try {
          var res = await fetch(window.location.pathname + "/data", {
            headers: { "x-admin-key": adminKey }
          });
          var data = await res.json();
          if (!data.ok) {
            status.className = "msg err";
            status.textContent = data.error || "Không tải được thống kê.";
            root.classList.add("hidden");
            return;
          }
          status.className = "msg ok";
          status.textContent = "Tải thống kê thành công.";
          render(data);
        } catch (err) {
          status.className = "msg err";
          status.textContent = "Không kết nối được tới máy chủ.";
          root.classList.add("hidden");
        } finally {
          btn.disabled = false;
        }
      });
    })();
  </script>
</body>
</html>
    `;
  }

  function renderPage(options = {}) {
    const title = esc(options.title || "Nhận Key Free");
    const heading = esc(options.heading || "Nhận Key Free");
    const sub = esc(options.sub || "");
    const alertClass =
      options.alertType === "err" ? "err" : options.alertType === "ok" ? "ok" : "";
    const alertText = options.alert ? esc(options.alert) : "";
    const freeNotice = esc(
      options.notice ||
        "Mỗi Ngày Lấy Được 1 Key Nhé Anh Em FF, APP sẽ được update về sau xịn hơn cho anh em chơi, muốn mua Files - Aimlock liên hệ cho Huy nhe"
    );
    const captchaMarkup = FREE_TURNSTILE_SITE_KEY
      ? `
        <div class="tile" style="margin-top:14px">
          <div class="sub" style="margin:0 0 10px">Xác minh captcha trước khi tạo phiên free.</div>
          <div class="cf-turnstile" data-sitekey="${esc(FREE_TURNSTILE_SITE_KEY)}" data-theme="dark"></div>
        </div>
      `
      : "";
    const keyBlock = options.key
      ? `
        <div class="tile" style="margin-top:14px">
          <div class="sub" style="margin:0 0 8px">Key đã nhận</div>
          <div id="freeKeyValue" style="font-size:30px;line-height:1.15;font-weight:700;word-break:break-word;color:#efe8ff">${esc(
            options.key
          )}</div>
          <div class="sub" style="margin-top:10px">Hạn dùng: ${esc(
            options.expireText || ""
          )}</div>
        </div>
        <div class="grid2" style="margin-top:12px">
          <button id="copyKeyBtn" class="btn" style="margin-top:0">Copy key</button>
          <a class="smallBtn" href="/panel" style="height:56px">Mở panel</a>
        </div>
      `
      : `
        <div class="tile" style="margin-top:14px">
          <div class="sub" style="margin:0">${freeNotice}</div>
        </div>
        ${captchaMarkup}
        <button id="freeStartBtn" class="btn" style="margin-top:14px">Get Key Free</button>
      `;

    const script = options.key
      ? `
        <script>
          (function () {
            var copyBtn = document.getElementById("copyKeyBtn");
            if (!copyBtn) return;
            copyBtn.addEventListener("click", async function () {
              var value = document.getElementById("freeKeyValue");
              if (!value) return;
              try {
                await navigator.clipboard.writeText(value.textContent.trim());
                copyBtn.textContent = "Đã copy";
              } catch (err) {
                copyBtn.textContent = "Copy lỗi";
              }
            });
          })();
        </script>
      `
      : `
        <script>
          (function () {
            function getBrowserId() {
              var key = "ath_free_browser_id";
              var current = "";
              try {
                current = localStorage.getItem(key) || "";
                if (!current) {
                  if (window.crypto && window.crypto.randomUUID) {
                    current = window.crypto.randomUUID();
                  } else {
                    current =
                      "ath-" +
                      Math.random().toString(36).slice(2) +
                      "-" +
                      Date.now().toString(36);
                  }
                  localStorage.setItem(key, current);
                }
              } catch (err) {
                current = "ath-fallback-" + Date.now().toString(36);
              }
              return current;
            }

            var button = document.getElementById("freeStartBtn");
            var status = document.getElementById("freeStatus");
            if (!button || !status) return;

            button.addEventListener("click", async function () {
              if (button.disabled) return;
              button.disabled = true;
              button.textContent = "Đang chuyển...";
              status.className = "msg";
              status.textContent = "Đang tạo phiên vượt link...";

              try {
                var captchaToken = "";
                if (window.turnstile) {
                  try {
                    captchaToken = window.turnstile.getResponse() || "";
                  } catch (innerErr) {
                    captchaToken = "";
                  }
                }
                var res = await fetch("/api/free/start", {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({
                    browserId: getBrowserId(),
                    turnstileToken: captchaToken
                  })
                });
                var data = await res.json();
                if (!data.ok) {
                  status.className = "msg err";
                  status.textContent = data.msg || "Không tạo được phiên";
                  button.disabled = false;
                  button.textContent = "Get Key Free";
                  return;
                }
                status.className = "msg ok";
                status.textContent = "Đang mở Link4m...";
                window.location.href = data.redirectUrl;
              } catch (err) {
                status.className = "msg err";
                status.textContent = "Không kết nối được tới máy chủ";
                button.disabled = false;
                button.textContent = "Get Key Free";
              }
            });
          })();
        </script>
      `;

    return `
<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">
  <meta name="theme-color" content="#08090c">
  <title>${title}</title>
  ${deps.baseStyles()}
  ${FREE_TURNSTILE_SITE_KEY ? '<script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer></script>' : ""}
  <style>
    .freeCard{max-width:640px}
  </style>
</head>
<body>
  <div class="bgAura"><div class="orb o1"></div><div class="orb o2"></div><div class="orb o3"></div></div>
  <div class="wrap">
    <div class="card freeCard">
      <div class="top">
        <div class="brand">
          <div class="logoBox">${deps.renderLogo(82, 24)}</div>
          <div>
            <h1 class="title">${heading}</h1>
            <div class="sub">${sub}</div>
          </div>
        </div>
      </div>
      <div class="content">
        <div class="noticeBox">
          ${freeNotice}
        </div>
        ${keyBlock}
        <div id="freeStatus" class="msg ${alertClass}" style="margin-top:14px">${alertText}</div>
      </div>
    </div>
  </div>
  ${script}
</body>
</html>
    `;
  }

  function createClaim(browserId, req) {
    const now = Date.now();
    const ip = getClientIp(req);
    const ua = getUserAgent(req);
    const browserHash = hashValue(browserId);
    const uaHash = hashValue(ua);
    const ipHash = hashValue(ip);
    const identityHash = hashValue(browserHash + "|" + uaHash + "|" + ipHash);
    const claimId = crypto.randomBytes(16).toString("hex");
    const expiresAt = now + FREE_CLAIM_TTL_MS;
    const claim = {
      id: claimId,
      createdAt: now,
      expiresAt,
      browserHash,
      uaHash,
      ipHash,
      identityHash,
      status: "started",
      verifiedAt: 0,
      claimedAt: 0,
      key: "",
      keyExpireAt: 0
    };
    return { claim, identityHash, browserHash };
  }

  function findActiveClaimByIdentity(identityHash) {
    const now = Date.now();
    return Object.values(freeStore.claims || {}).find((claim) => {
      if (!claim || typeof claim !== "object") return false;
      if (claim.identityHash !== identityHash) return false;
      if (claim.status === "claimed") return false;
      return Number(claim.expiresAt || 0) > now;
    });
  }

  async function issueKeyForClaim(claim) {
    const allKeys =
      typeof deps.getAllKeys === "function" ? deps.getAllKeys() : deps.getKeys();
    if (claim.key && allKeys[claim.key]) {
      return {
        key: claim.key,
        expireAt: Number(claim.keyExpireAt || 0)
      };
    }

    let nextKey = "";
    do {
      nextKey = deps.genKey();
    } while (allKeys[nextKey]);

    const expireAt = Date.now() + FREE_KEY_DAYS * 24 * 60 * 60 * 1000;
    freeKeys[nextKey] = {
      usesLeft: FREE_KEY_USES,
      totalDevices: FREE_KEY_USES,
      devices: [],
      expireAt,
      createdAt: Date.now(),
      source: "free"
    };

    const claimedAt = Date.now();
    freeStore.claims[claim.id] = {
      id: claim.id,
      createdAt: Number(claim.createdAt || claimedAt),
      expiresAt: Number(claim.expiresAt || claimedAt),
      browserHash: "",
      uaHash: "",
      ipHash: "",
      identityHash: String(claim.identityHash || ""),
      status: "claimed",
      verifiedAt: Number(claim.verifiedAt || claimedAt),
      claimedAt,
      key: nextKey,
      keyExpireAt: expireAt
    };
    freeStore.cooldowns[claim.identityHash] = Date.now() + FREE_COOLDOWN_MS;

    await saveFreeKeys();
    await saveStore();

    return { key: nextKey, expireAt };
  }

  app.use((req, res, next) => {
    if (!req.path.startsWith(`/${FREE_ROUTE}`) && !req.path.startsWith("/api/free/")) {
      return next();
    }
    if (!freeReady) {
      return res.status(503).send(
        renderPage({
          title: "Free key đang khởi động",
          heading: "Free key đang khởi động",
          alertType: "err",
          alert: "Hệ thống free đang khởi động, thử lại sau ít giây."
        })
      );
    }
    next();
  });

  app.use((req, res, next) => {
    if (!req.path.startsWith("/api/free/")) return next();
    const ipHash = hashValue(getClientIp(req));
    const bucketKey = `${ipHash}:${req.path}`;
    const now = Date.now();
    const rule =
      req.path === "/api/free/start"
        ? { windowMs: 10 * 60 * 1000, limit: 12 }
        : { windowMs: 10 * 60 * 1000, limit: 30 };
    const recent = (freeRateMap.get(bucketKey) || []).filter(
      (time) => now - time < rule.windowMs
    );
    recent.push(now);
    freeRateMap.set(bucketKey, recent);
    if (freeRateMap.size > FREE_RATE_BUCKET_MAX_KEYS || Math.random() < 0.1) {
      for (const [key, values] of freeRateMap.entries()) {
        const filtered = values.filter((time) => now - time < FREE_RATE_BUCKET_TTL_MS);
        if (filtered.length) freeRateMap.set(key, filtered);
        else freeRateMap.delete(key);
      }
      if (freeRateMap.size > FREE_RATE_BUCKET_MAX_KEYS) {
        const keys = Array.from(freeRateMap.keys());
        const overflow = freeRateMap.size - FREE_RATE_BUCKET_MAX_KEYS;
        for (let i = 0; i < overflow; i += 1) {
          freeRateMap.delete(keys[i]);
        }
      }
    }
    if (recent.length > rule.limit) {
      logEvent("rate_limit", req, { bucketKey, count: recent.length });
      return res.status(429).json({
        ok: false,
        msg: "Bạn thao tác quá nhanh, chờ một lúc rồi thử lại."
      });
    }
    next();
  });

  app.get(`/${FREE_ROUTE}`, (req, res) => {
    res.send(renderPage());
  });

  if (FREE_STATS_ROUTE) {
    app.get(`/${FREE_STATS_ROUTE}`, (req, res) => {
      res.send(renderStatsPage());
    });

    app.get(`/${FREE_STATS_ROUTE}/data`, (req, res) => {
      if (typeof deps.isAdmin === "function" && !deps.isAdmin(req)) {
        return res.status(401).json({ ok: false, error: "Sai admin key" });
      }
      return res.json({
        ok: true,
        route: `/${FREE_STATS_ROUTE}`,
        freeRoute: `/${FREE_ROUTE}`,
        ...getStatsSnapshot()
      });
    });
  }

  app.post("/api/free/start", async (req, res) => {
    if (cleanupExpiredFreeKeys()) {
      await saveFreeKeys();
    }
    const browserId = String(req.body.browserId || "").trim();
    const turnstileToken = String(req.body.turnstileToken || "").trim();
    if (!browserId || browserId.length < 8) {
      logEvent("start_invalid_browser", req);
      return res.status(400).json({ ok: false, msg: "Browser ID không hợp lệ." });
    }

    if (FREE_TURNSTILE_SITE_KEY || FREE_TURNSTILE_SECRET_KEY) {
      if (!FREE_TURNSTILE_SITE_KEY || !FREE_TURNSTILE_SECRET_KEY) {
        logEvent("turnstile_misconfigured", req);
        return res.status(500).json({
          ok: false,
          msg: "Captcha đang bị cấu hình thiếu."
        });
      }
      if (!turnstileToken) {
        logEvent("turnstile_missing", req);
        return res.status(400).json({
          ok: false,
          msg: "Vui long xac minh captcha."
        });
      }
      try {
        const captchaResult = await verifyTurnstileToken(turnstileToken, req);
        if (!captchaResult.ok) {
          logEvent("turnstile_failed", req, { codes: captchaResult.codes });
          return res.status(403).json({
            ok: false,
            msg: "Captcha không hợp lệ, vui lòng thử lại."
          });
        }
      } catch (err) {
        logEvent("turnstile_error", req, { error: err.message });
        return res.status(502).json({
          ok: false,
          msg: "Không xác minh được captcha."
        });
      }
    }

    cleanupFreeStore();

    const { claim, identityHash, browserHash } = createClaim(browserId, req);
    const cooldownUntil = Number(freeStore.cooldowns[identityHash] || 0);
    if (cooldownUntil > Date.now()) {
      logEvent("cooldown_hit", req, {
        identityHash,
        retryAfterMs: cooldownUntil - Date.now()
      });
      return res.status(429).json({
        ok: false,
        msg: `Bạn vừa nhận key free. Thử lại sau ${formatDuration(
          cooldownUntil - Date.now()
        )}.`
      });
    }

    const activeClaim = findActiveClaimByIdentity(identityHash);
    if (activeClaim) {
      logEvent("active_claim_exists", req, {
        claimId: activeClaim.id,
        identityHash,
        claimExpiresAt: activeClaim.expiresAt
      });
      return res.status(429).json({
        ok: false,
        msg: "Bạn đang có một phiên vượt link chưa xong, vui lòng hoàn tất hoặc chờ hết hạn."
      });
    }

    freeStore.claims[claim.id] = claim;
    await saveStore();
    logEvent("claim_started", req, {
      claimId: claim.id,
      claimExpiresAt: claim.expiresAt,
      identityHash
    });

    const state = createToken({
      claimId: claim.id,
      browserHash,
      expiresAt: claim.expiresAt
    });
    const callbackUrl = `${getBaseUrl(req)}/api/free/callback?state=${encodeURIComponent(
      state
    )}`;
    let redirectUrl = "";
    try {
      redirectUrl = await createLink4mUrl(callbackUrl);
    } catch (err) {
      logEvent("link4m_create_failed", req, {
        claimId: claim.id,
        error: err.message
      });
      return res.status(500).json({
        ok: false,
        msg: err.message || "Không tạo được link Link4m."
      });
    }

    setCookie(
      res,
      req,
      "ath_free_claim",
      createToken({
        claimId: claim.id,
        browserHash,
        expiresAt: claim.expiresAt
      }),
      FREE_CLAIM_TTL_MS
    );

    return res.json({
      ok: true,
      redirectUrl,
      expiresAt: claim.expiresAt
    });
  });

  app.get("/api/free/callback", async (req, res) => {
    if (cleanupExpiredFreeKeys()) {
      await saveFreeKeys();
    }
    cleanupFreeStore();
    const finishCookie = function () {
      clearCookie(res, req, "ath_free_claim");
    };

    const state = verifyToken(String(req.query.state || "").trim());
    if (!state || !state.claimId || !state.browserHash || !state.expiresAt) {
      logEvent("callback_invalid_state", req);
      finishCookie();
      return res.status(400).send(
        renderPage({
          title: "Phiên free không hợp lệ",
          heading: "Phiên free không hợp lệ",
          alertType: "err",
          alert: "Link callback không hợp lệ hoặc đã bị sửa."
        })
      );
    }

    const cookiePayload = verifyToken(parseCookies(req).ath_free_claim || "");
    if (
      !cookiePayload ||
      cookiePayload.claimId !== state.claimId ||
      cookiePayload.browserHash !== state.browserHash
    ) {
      logEvent("callback_cookie_mismatch", req, {
        claimId: state.claimId
      });
      finishCookie();
      return res.status(403).send(
        renderPage({
          title: "Không xác minh được phiên",
          heading: "Không xác minh được phiên",
          alertType: "err",
          alert: "Phiên free không đúng trình duyệt đã bắt đầu."
        })
      );
    }

    const claim = freeStore.claims[state.claimId];
    if (!claim) {
      logEvent("callback_missing_claim", req, {
        claimId: state.claimId
      });
      finishCookie();
      return res.status(404).send(
        renderPage({
          title: "Phiên đã hết hạn",
          heading: "Phiên đã hết hạn",
          alertType: "err",
          alert: "Phiên vượt link này đã hết hạn hoặc đã bị xóa."
        })
      );
    }

    const now = Date.now();
    if (claim.expiresAt < now || Number(state.expiresAt) < now) {
      logEvent("callback_expired", req, {
        claimId: state.claimId
      });
      finishCookie();
      return res.status(410).send(
        renderPage({
          title: "Phiên đã hết hạn",
          heading: "Phiên đã hết hạn",
          alertType: "err",
          alert: "Bạn cần bắt đầu lại một phiên free mới."
        })
      );
    }

    if (claim.browserHash !== state.browserHash) {
      logEvent("callback_browser_mismatch", req, {
        claimId: state.claimId
      });
      finishCookie();
      return res.status(403).send(
        renderPage({
          title: "Sai trình duyệt",
          heading: "Sai trình duyệt",
          alertType: "err",
          alert: "Phiên này không được mở bằng trình duyệt đã bắt đầu."
        })
      );
    }

    if (FREE_STRICT_IP_MATCH && claim.ipHash !== hashValue(getClientIp(req))) {
      logEvent("callback_ip_mismatch", req, {
        claimId: state.claimId
      });
      finishCookie();
      return res.status(403).send(
        renderPage({
          title: "Sai IP",
          heading: "Sai IP",
          alertType: "err",
          alert: "Phiên free bị thay đổi mạng hoặc IP, vui lòng bắt đầu lại."
        })
      );
    }

    if (claim.uaHash !== hashValue(getUserAgent(req))) {
      logEvent("callback_ua_mismatch", req, {
        claimId: state.claimId
      });
      finishCookie();
      return res.status(403).send(
        renderPage({
          title: "Sai User-Agent",
          heading: "Sai User-Agent",
          alertType: "err",
          alert: "Phiên free bị thay đổi thiết bị hoặc trình duyệt."
        })
      );
    }

    if (now - Number(claim.createdAt || 0) < FREE_MIN_ELAPSED_MS) {
      logEvent("callback_too_fast", req, {
        claimId: state.claimId,
        elapsedMs: now - Number(claim.createdAt || 0)
      });
      finishCookie();
      return res.status(403).send(
        renderPage({
          title: "Hoàn tất quá nhanh",
          heading: "Hoàn tất quá nhanh",
          alertType: "err",
          alert: "Phiên này quay về quá nhanh, vui lòng vượt Link4m đúng cách."
        })
      );
    }

    const referer = String(req.headers.referer || "").toLowerCase();
    if (
      FREE_REFERER_KEYWORD &&
      referer &&
      !referer.includes(FREE_REFERER_KEYWORD)
    ) {
      logEvent("callback_bad_referer", req, {
        claimId: state.claimId,
        refererHash: hashValue(referer)
      });
      finishCookie();
      return res.status(403).send(
        renderPage({
          title: "Referer không hợp lệ",
          heading: "Referer không hợp lệ",
          alertType: "err",
          alert: "Callback không đến từ Link4m."
        })
      );
    }

    claim.status = "verified";
    claim.verifiedAt = now;
    logEvent("claim_verified", req, {
      claimId: claim.id,
      identityHash: claim.identityHash
    });

    const issued = await issueKeyForClaim(claim);
    logEvent("claim_issued", req, {
      claimId: claim.id,
      identityHash: claim.identityHash,
      key: issued.key,
      expireAt: issued.expireAt
    });
    finishCookie();
    return res.send(
      renderPage({
        title: "Nhận key thành công",
        heading: "Nhận key thành công",
        sub: "Key được tạo tự động sau khi callback hợp lệ.",
        alertType: "ok",
        alert: "Key này đã được ghi vào hệ thống key hiện tại.",
        key: issued.key,
        expireText: deps.formatVNTime(issued.expireAt)
      })
    );
  });

  if (deps.freeRuntime && typeof deps.freeRuntime === "object") {
    deps.freeRuntime.getKeys = () => freeKeys;
    deps.freeRuntime.getStatsSnapshot = () => cloneJson(getStatsSnapshot());
    deps.freeRuntime.syncUsageCleanup = (key) =>
      syncFreeHousekeepingInBackground({ keepKey: key });
    deps.freeRuntime.findKey = (key) => {
      const item = freeKeys[String(key || "").trim()];
      if (!item) return null;
      return typeof deps.normalizeKeyItem === "function"
        ? deps.normalizeKeyItem(item)
        : item;
    };
    deps.freeRuntime.deleteKey = (key, options = {}) => removeFreeKey(key, options);
    deps.freeRuntime.saveKey = (key, item, options = {}) => {
      const normalized =
        typeof deps.normalizeKeyItem === "function"
          ? deps.normalizeKeyItem(item)
          : item;
      if (!normalized) return Promise.resolve();
      freeKeys[String(key || "").trim()] = normalized;
      return saveFreeKeys(options);
    };
    deps.freeRuntime.getHealth = () => {
      const stats = getStatsSnapshot();
      return {
        ready: freeReady,
        mode: hasFreeGithubStore() ? "github" : "local",
        repo: FREE_REPO_CONFIG.repo || "",
        branch: FREE_REPO_CONFIG.branch || "",
        keyCount: Object.keys(freeKeys || {}).length,
        activeCooldowns: stats.activeCooldowns,
        pendingClaims: stats.pendingClaims,
        statsDays: stats.statsDays || 0,
        statsRouteEnabled: !!FREE_STATS_ROUTE
      };
    };
  }

  const ready = Promise.all([initStore(), initFreeKeysStore()])
    .then(async ([storeChanged, freeKeysChanged]) => {
      await migrateLegacyFreeKeys();
      if (storeChanged) {
        saveStore({ background: true }).catch(() => {});
      }
      if (freeKeysChanged) {
        saveFreeKeys({ background: true }).catch(() => {});
      }
    })
    .then(() => {
      freeReady = true;
      startFreeHousekeeping();
    })
    .catch((err) => {
      console.error("Init free key routes failed:", err.message);
    });

  return { ready };
};
