
const express = require("express");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const https = require("https");
const registerFreeKeyRoutes = require("./free-key");

const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname)));

const PORT = process.env.PORT || 3000;
const ADMIN_KEY = String(process.env.ADMIN_PASSWORD || "").trim();
const SESSION_SECRET = String(process.env.SESSION_SECRET || crypto.randomBytes(32).toString("hex"));

const FACEBOOK_URL =
  "https://www.facebook.com/share/1JHonUUaCA/?mibextid=wwXIfr";
const ZALO_URL = "https://zalo.me/0818249250";
const TIKTOK_URL =
  "https://www.tiktok.com/@huyftsupport?_r=1&_t=ZS-94olc9q74ba";
const YOUTUBE_URL = "https://www.youtube.com/@THGaming.";

const FF_URL = process.env.FF_URL || "https://ff.garena.com/vn/";
const FF_MAX_URL = process.env.FF_MAX_URL || "https://ff.garena.com/vn/";
const FF_ANDROID_PACKAGE =
  process.env.FF_ANDROID_PACKAGE || "com.dts.freefireth";
const FFMAX_ANDROID_PACKAGE =
  process.env.FFMAX_ANDROID_PACKAGE || "com.dts.freefiremax";
const FF_IOS_SCHEME = process.env.FF_IOS_SCHEME || "freefire://";
const FFMAX_IOS_SCHEME = process.env.FFMAX_IOS_SCHEME || "freefiremax://";
const FF_IOS_APPID = process.env.FF_IOS_APPID || "1300146617";
const FFMAX_IOS_APPID = process.env.FFMAX_IOS_APPID || "1480516829";

const STORE_PATH = path.join(__dirname, "keys.json");
const EXPIRED_MAIN_KEYS_PATH = path.join(__dirname, ".data", "expired_main_keys.json");
const DEVICE_STATS_PATH = path.join(__dirname, ".data", "device_daily_stats.json");
const LOGO_PATH = path.join(__dirname, "logo.png");
const rateMap = new Map();

const MAIN_GITHUB_TOKEN = process.env.MAIN_GITHUB_TOKEN || process.env.GITHUB_TOKEN || "";
const MAIN_GITHUB_REPO = process.env.MAIN_GITHUB_REPO || process.env.GITHUB_REPO || "";
const MAIN_GITHUB_BRANCH = process.env.MAIN_GITHUB_BRANCH || process.env.GITHUB_BRANCH || "main";
const MAIN_GITHUB_DATA_PATH =
  process.env.MAIN_GITHUB_DATA_PATH || process.env.GITHUB_DATA_PATH || "keys.json";
const MAIN_EXPIRED_GITHUB_DATA_PATH =
  process.env.MAIN_EXPIRED_GITHUB_DATA_PATH || ".data/expired_main_keys.json";
const MAIN_DEVICE_STATS_GITHUB_DATA_PATH =
  process.env.MAIN_DEVICE_STATS_GITHUB_DATA_PATH || ".data/device_daily_stats.json";
const FREE_GITHUB_TOKEN =
  process.env.FREE_GITHUB_TOKEN || process.env.MAIN_GITHUB_TOKEN || process.env.GITHUB_TOKEN || "";
const FREE_GITHUB_REPO =
  process.env.FREE_GITHUB_REPO || process.env.MAIN_GITHUB_REPO || process.env.GITHUB_REPO || "";
const FREE_GITHUB_BRANCH =
  process.env.FREE_GITHUB_BRANCH || process.env.MAIN_GITHUB_BRANCH || process.env.GITHUB_BRANCH || "main";
const ADMIN_ROUTE = String(process.env.ADMIN_ROUTE || "nhimne").replace(/^\/+|\/+$/g, "") || "nhimne";
const VN_TIMEZONE = "Asia/Ho_Chi_Minh";
const PERMANENT_KEY_DAYS = 9999;
const APP_BUILD_TAG = new Date().toISOString();
const ADMIN_SESSION_COOKIE = "ath_admin_session";
const ADMIN_SESSION_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const RATE_BUCKET_TTL_MS = Math.max(
  60 * 1000,
  Number(process.env.RATE_BUCKET_TTL_MS || 2 * 60 * 1000)
);
const RATE_BUCKET_MAX_KEYS = Math.max(
  200,
  Number(process.env.RATE_BUCKET_MAX_KEYS || 3000)
);
const freeRuntime = {};
const mainStoreConfig = {
  token: MAIN_GITHUB_TOKEN,
  repo: MAIN_GITHUB_REPO,
  branch: MAIN_GITHUB_BRANCH,
  dataPath: MAIN_GITHUB_DATA_PATH
};
const freeStoreConfig = {
  token: FREE_GITHUB_TOKEN,
  repo: FREE_GITHUB_REPO,
  branch: FREE_GITHUB_BRANCH
};

let keys = {};
let storeReady = false;
let saveQueue = Promise.resolve();
let lastSyncedMainKeys = {};
let expiredMainKeys = {};
let expiredMainKeysSaveQueue = Promise.resolve();
let deviceDailyStats = { days: {} };
let deviceDailyStatsSaveQueue = Promise.resolve();

function loadLocalStore() {
  try {
    if (!fs.existsSync(STORE_PATH)) return {};
    const raw = fs.readFileSync(STORE_PATH, "utf8");
    const parsed = JSON.parse(raw || "{}");
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function loadLocalExpiredMainKeys() {
  try {
    fs.mkdirSync(path.dirname(EXPIRED_MAIN_KEYS_PATH), { recursive: true });
    if (!fs.existsSync(EXPIRED_MAIN_KEYS_PATH)) return {};
    const raw = fs.readFileSync(EXPIRED_MAIN_KEYS_PATH, "utf8");
    const parsed = JSON.parse(raw || "{}");
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function loadLocalDeviceDailyStats() {
  try {
    fs.mkdirSync(path.dirname(DEVICE_STATS_PATH), { recursive: true });
    if (!fs.existsSync(DEVICE_STATS_PATH)) return { days: {} };
    const raw = fs.readFileSync(DEVICE_STATS_PATH, "utf8");
    const parsed = JSON.parse(raw || "{}");
    return parsed && typeof parsed === "object" ? parsed : { days: {} };
  } catch {
    return { days: {} };
  }
}

function saveLocalStore() {
  fs.writeFileSync(STORE_PATH, JSON.stringify(keys, null, 2), "utf8");
}

function saveLocalExpiredMainKeys(store) {
  fs.mkdirSync(path.dirname(EXPIRED_MAIN_KEYS_PATH), { recursive: true });
  fs.writeFileSync(EXPIRED_MAIN_KEYS_PATH, JSON.stringify(store, null, 2), "utf8");
}

function saveLocalDeviceDailyStats(store) {
  fs.mkdirSync(path.dirname(DEVICE_STATS_PATH), { recursive: true });
  fs.writeFileSync(DEVICE_STATS_PATH, JSON.stringify(store, null, 2), "utf8");
}

function resolveStoreConfig(config = mainStoreConfig) {
  return {
    token: String(config && config.token ? config.token : ""),
    repo: String(config && config.repo ? config.repo : ""),
    branch: String(config && config.branch ? config.branch : "main") || "main",
    dataPath: String(config && config.dataPath ? config.dataPath : "")
  };
}

function hasGithubStore(config = mainStoreConfig) {
  const resolved = resolveStoreConfig(config);
  return Boolean(resolved.token && resolved.repo && resolved.dataPath);
}

function getStoreMode(config = mainStoreConfig) {
  return hasGithubStore(config) ? "github" : "local";
}

function githubRequest(method, apiPath, body, config = mainStoreConfig) {
  return new Promise((resolve, reject) => {
    const resolved = resolveStoreConfig(config);
    console.log("[CALL GITHUB]", method, apiPath);
    console.log("[TOKEN PREFIX]", resolved.token ? resolved.token.slice(0, 10) : "(empty)");

    const req = https.request(
      {
        hostname: "api.github.com",
        path: apiPath,
        method,
        timeout: 10000,
        headers: {
          "User-Agent": "aimtrickhead-panel",
          Authorization: `Bearer ${resolved.token}`,
          Accept: "application/vnd.github+json",
          "X-GitHub-Api-Version": "2022-11-28",
          "Content-Type": "application/json"
        }
      },
      (res) => {
        console.log("[STATUS]", res.statusCode);
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          console.log("[RESPONSE]", (data || "").slice(0, 300));
          let parsed = {};
          try {
            parsed = JSON.parse(data || "{}");
          } catch (parseErr) {
            console.log("[GITHUB JSON PARSE ERROR]", parseErr.message);
            parsed = {};
          }

          if (res.statusCode >= 200 && res.statusCode < 300) {
            return resolve(parsed);
          }

          const err = new Error(parsed.message || `GitHub ${res.statusCode}`);
          err.statusCode = res.statusCode;
          err.payload = parsed;
          reject(err);
        });
      }
    );

    req.on("error", (err) => {
      console.log("[REQUEST ERROR]", err.message);
      reject(err);
    });
    req.on("timeout", () => req.destroy(new Error("GitHub request timeout")));
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

async function readGithubStore(dataPath = mainStoreConfig.dataPath, config = mainStoreConfig) {
  const resolved = resolveStoreConfig({ ...config, dataPath });
  console.log("=== READ GITHUB STORE ===");
  console.log("REPO:", resolved.repo);
  console.log("BRANCH:", resolved.branch);
  console.log("PATH:", resolved.dataPath);
  const [owner, repo] = String(resolved.repo || "").split("/");
  if (!owner || !repo) throw new Error("GITHUB_REPO không hợp lệ");

  const apiPath = `/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/contents/${resolved.dataPath
    .split("/")
    .map(encodeURIComponent)
    .join("/")}?ref=${encodeURIComponent(resolved.branch)}`;

  try {
    const file = await githubRequest("GET", apiPath, null, resolved);
    const rawContent = String(file && file.content ? file.content : "").replace(/\s+/g, "");
    const content = Buffer.from(rawContent, "base64").toString("utf8");
    const parsed = JSON.parse(content || "{}");
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (err) {
    console.log("[READ GITHUB STORE ERROR]", err.statusCode || "no-status", err.message);
    if (err.statusCode === 404) {
      return {};
    }
    throw err;
  }
}

async function writeGithubStore(
  store,
  dataPath = mainStoreConfig.dataPath,
  commitMessage = "DANG cap nhat key",
  config = mainStoreConfig
) {
  const resolved = resolveStoreConfig({ ...config, dataPath });
  const apiPath = `/repos/${encodeURIComponent(
    resolved.repo.split("/")[0]
  )}/${encodeURIComponent(resolved.repo.split("/")[1])}/contents/${resolved.dataPath
    .split("/")
    .map(encodeURIComponent)
    .join("/")}`;

  let sha = undefined;
  try {
    const existing = await githubRequest(
      "GET",
      `${apiPath}?ref=${encodeURIComponent(resolved.branch)}`,
      null,
      resolved
    );
    sha = existing.sha;
  } catch (err) {
    if (err.statusCode !== 404) throw err;
  }

  const body = {
    message: commitMessage || "Cập nhật key",
    content: Buffer.from(JSON.stringify(store, null, 2), "utf8").toString("base64"),
    branch: resolved.branch
  };
  if (sha) body.sha = sha;

  await githubRequest("PUT", apiPath, body, resolved);
}

function normalizeKeyItem(item) {
  if (!item || typeof item !== "object") return null;

  const out = { ...item };

  if (!Array.isArray(out.devices)) {
    if (Array.isArray(out.usedDevices)) out.devices = [...out.usedDevices];
    else out.devices = [];
  }
  if (out.device && !out.devices.includes(out.device)) out.devices.push(out.device);

  if (typeof out.usesLeft !== "number") {
    if (typeof out.uses === "number") out.usesLeft = Number(out.uses || 0);
    else if (typeof out.maxDevices === "number") {
      out.usesLeft = Math.max(0, Number(out.maxDevices) - out.devices.length);
    } else out.usesLeft = 0;
  }

  if (typeof out.totalDevices !== "number") {
    if (typeof out.maxDevices === "number") out.totalDevices = Number(out.maxDevices || 0);
    else {
      out.totalDevices = Math.max(
        out.devices.length,
        out.devices.length + Number(out.usesLeft || 0)
      );
    }
  }

  out.usesLeft = Math.max(0, Number(out.usesLeft || 0));
  out.totalDevices = Math.max(out.devices.length, Number(out.totalDevices || 0));
  out.expireAt = Number(out.expireAt || out.expiresAt || 0);
  out.createdAt = Number(out.createdAt || Date.now());

  delete out.device;
  delete out.uses;
  delete out.usedDevices;
  delete out.maxDevices;
  delete out.expiresAt;

  return out;
}

function normalizeAllStore(store) {
  const out = {};
  Object.keys(store || {}).forEach((k) => {
    const normalized = normalizeKeyItem(store[k]);
    if (normalized) out[k] = normalized;
  });
  return out;
}

function normalizeExpiredKeyStore(store) {
  const out = {};
  Object.keys(store || {}).forEach((key) => {
    const expireAt = Number(store[key] || 0);
    if (expireAt > 0) out[key] = expireAt;
  });
  return out;
}

function createEmptyDeviceDay() {
  return {
    admin: { android: {}, ios: {} },
    free: { android: {}, ios: {} }
  };
}

function normalizeDeviceDailyStats(store) {
  const out = { days: {} };
  Object.keys((store && store.days) || {}).forEach((dayKey) => {
    const rawDay = store.days[dayKey] || {};
    const nextDay = createEmptyDeviceDay();
    ["admin", "free"].forEach((scope) => {
      ["android", "ios"].forEach((platform) => {
        const devices = rawDay[scope] && rawDay[scope][platform] ? rawDay[scope][platform] : {};
        const clean = {};
        Object.keys(devices || {}).forEach((deviceHash) => {
          clean[String(deviceHash)] = Number(devices[deviceHash] || Date.now());
        });
        nextDay[scope][platform] = clean;
      });
    });
    out.days[String(dayKey)] = nextDay;
  });
  return out;
}

function cloneStore(store) {
  return JSON.parse(JSON.stringify(store || {}));
}

function applyExternalDeletions(snapshot, latestStore, lastSyncedStore) {
  const next = cloneStore(snapshot);
  Object.keys(lastSyncedStore || {}).forEach((key) => {
    if (!latestStore[key] && next[key]) {
      delete next[key];
    }
  });
  return next;
}

function cleanupExpiredMainKeyMarkers() {
  const now = Date.now();
  const keep = {};
  Object.keys(expiredMainKeys || {}).forEach((key) => {
    const expireAt = Number(expiredMainKeys[key] || 0);
    if (expireAt > now - 30 * 24 * 60 * 60 * 1000) {
      keep[key] = expireAt;
    }
  });
  expiredMainKeys = keep;
}

function getVNDayKey(value = Date.now()) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: VN_TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).format(new Date(value));
}

function cleanupDeviceDailyStats() {
  const today = new Date();
  const keepAfter = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
  const keepDay = getVNDayKey(keepAfter.getTime());
  const next = { days: {} };
  Object.keys((deviceDailyStats && deviceDailyStats.days) || {}).forEach((dayKey) => {
    if (dayKey >= keepDay) {
      next.days[dayKey] = deviceDailyStats.days[dayKey];
    }
  });
  deviceDailyStats = normalizeDeviceDailyStats(next);
}

function detectPlatformFromRequest(req) {
  const ua = String(req.headers["user-agent"] || "");
  if (/Android/i.test(ua)) return "android";
  if (/iPhone|iPad|iPod/i.test(ua)) return "ios";
  return "other";
}

function isNativeApkClient(req) {
  const ua = String(req.headers["user-agent"] || "");
  const clientTag = String(req.headers["x-aimtrickhead-client"] || "")
    .trim()
    .toLowerCase();
  return clientTag === "native-apk" || /AimTrickHeadNative/i.test(ua);
}

function shouldShowAndroidUpdate(req) {
  return detectPlatformFromRequest(req) === "android" && !isNativeApkClient(req);
}

function recordDailyDeviceUsage(scope, platform, device) {
  if (!scope || !platform || !device) return;
  if (scope !== "admin" && scope !== "free") return;
  if (platform !== "android" && platform !== "ios") return;
  cleanupDeviceDailyStats();
  const dayKey = getVNDayKey();
  if (!deviceDailyStats.days[dayKey]) {
    deviceDailyStats.days[dayKey] = createEmptyDeviceDay();
  }
  const deviceHash = crypto.createHash("sha256").update(String(device)).digest("hex");
  deviceDailyStats.days[dayKey][scope][platform][deviceHash] = Date.now();
}

function getTodayDeviceUsageSummary() {
  cleanupDeviceDailyStats();
  const dayKey = getVNDayKey();
  const day = deviceDailyStats.days[dayKey] || createEmptyDeviceDay();
  return {
    day: dayKey,
    admin: {
      android: Object.keys(day.admin.android || {}).length,
      ios: Object.keys(day.admin.ios || {}).length
    },
    free: {
      android: Object.keys(day.free.android || {}).length,
      ios: Object.keys(day.free.ios || {}).length
    }
  };
}

function getFreeTodaySummary() {
  const fallbackDay = getVNDayKey();
  if (!freeRuntime || typeof freeRuntime.getStatsSnapshot !== "function") {
    return {
      day: fallbackDay,
      verified: 0,
      issued: 0,
      verifiedDevices: 0,
      issuedDevices: 0
    };
  }
  const snapshot = freeRuntime.getStatsSnapshot();
  const today = snapshot && snapshot.today ? snapshot.today : {};
  const verified = Math.max(0, Number(today.verified || 0));
  const issued = Math.max(0, Number(today.issued || 0));
  const verifiedDevices = Math.max(0, Number(today.verifiedDevices || 0)) || verified;
  const issuedDevices = Math.max(0, Number(today.issuedDevices || 0)) || issued;
  return {
    day: String(today.day || fallbackDay),
    verified,
    issued,
    verifiedDevices,
    issuedDevices
  };
}

function cleanupExpiredMainKeys() {
  const now = Date.now();
  let changed = false;
  Object.keys(keys || {}).forEach((key) => {
    const item = normalizeKeyItem(keys[key]);
    if (!item) {
      delete keys[key];
      changed = true;
      return;
    }
    if (item.expireAt > 0 && item.expireAt <= now) {
      delete keys[key];
      expiredMainKeys[key] = item.expireAt;
      changed = true;
      return;
    }
    keys[key] = item;
  });
  cleanupExpiredMainKeyMarkers();
  return changed;
}

function syncExpiredMainKeysInBackground() {
  if (!cleanupExpiredMainKeys()) return false;
  saveStore({ background: true }).catch((err) => {
    console.error("Background save failed:", err.message);
  });
  saveExpiredMainKeys({ background: true }).catch((err) => {
    console.error("Background save failed:", err.message);
  });
  return true;
}

function isExpiredDeletedMainKey(key) {
  const expireAt = Number(expiredMainKeys[String(key || "").trim()] || 0);
  return expireAt > 0;
}

async function persistExpiredMainKeys(snapshot) {
  try {
    if (hasGithubStore(mainStoreConfig)) {
      await writeGithubStore(
        snapshot,
        MAIN_EXPIRED_GITHUB_DATA_PATH,
        "Cập nhật expired main keys",
        mainStoreConfig
      );
    } else {
      saveLocalExpiredMainKeys(snapshot);
    }
  } catch (err) {
    console.error("Persist expired main keys failed:", err.message);
    saveLocalExpiredMainKeys(snapshot);
  }
}

function saveExpiredMainKeys(options = {}) {
  const background = !!options.background;
  cleanupExpiredMainKeyMarkers();
  expiredMainKeys = normalizeExpiredKeyStore(expiredMainKeys);
  const snapshot = cloneStore(expiredMainKeys);
  expiredMainKeysSaveQueue = expiredMainKeysSaveQueue
    .then(() => persistExpiredMainKeys(snapshot))
    .catch((err) => {
      console.error("Expired main keys queue failed:", err.message);
    });
  if (!background) return expiredMainKeysSaveQueue;
  return Promise.resolve();
}

async function persistDeviceDailyStats(snapshot) {
  try {
    if (hasGithubStore(mainStoreConfig)) {
      await writeGithubStore(
        snapshot,
        MAIN_DEVICE_STATS_GITHUB_DATA_PATH,
        "Cập nhật device daily stats",
        mainStoreConfig
      );
    } else {
      saveLocalDeviceDailyStats(snapshot);
    }
  } catch (err) {
    console.error("Persist device daily stats failed:", err.message);
    saveLocalDeviceDailyStats(snapshot);
  }
}

function saveDeviceDailyStats(options = {}) {
  const background = !!options.background;
  cleanupDeviceDailyStats();
  deviceDailyStats = normalizeDeviceDailyStats(deviceDailyStats);
  const snapshot = cloneStore(deviceDailyStats);
  deviceDailyStatsSaveQueue = deviceDailyStatsSaveQueue
    .then(() => persistDeviceDailyStats(snapshot))
    .catch((err) => {
      console.error("Device daily stats queue failed:", err.message);
    });
  if (!background) return deviceDailyStatsSaveQueue;
  return Promise.resolve();
}

function getAllKeysSnapshot() {
  const freeKeys =
    freeRuntime && typeof freeRuntime.getKeys === "function"
      ? normalizeAllStore(freeRuntime.getKeys())
      : {};
  return { ...freeKeys, ...keys };
}

function findKeyRecord(key) {
  const mainItem = normalizeKeyItem(keys[key]);
  if (mainItem) return { scope: "main", item: mainItem };

  if (freeRuntime && typeof freeRuntime.findKey === "function") {
    const freeItem = normalizeKeyItem(freeRuntime.findKey(key));
    if (freeItem) return { scope: "free", item: freeItem };
  }

  return null;
}

function saveKeyRecord(scope, key, item, options = {}) {
  const normalized = normalizeKeyItem(item);
  if (!normalized) return Promise.resolve();

  if (scope === "free") {
    if (freeRuntime && typeof freeRuntime.saveKey === "function") {
      return freeRuntime.saveKey(key, normalized, options);
    }
    return Promise.resolve();
  }

  keys[key] = normalized;
  return saveStore(options);
}

function getAdminKeyItems() {
  cleanupExpiredMainKeys();
  keys = normalizeAllStore(keys);
  return Object.entries(keys)
    .map(([key, value]) => ({
      key,
      usesLeft: Number(value.usesLeft || 0),
      usedDevices: Array.isArray(value.devices) ? value.devices.length : 0,
      totalDevices: Number(value.totalDevices || 0),
      expireAt: Number(value.expireAt || 0),
      expireText: formatVNTime(Number(value.expireAt || 0)),
      createdAt: Number(value.createdAt || 0)
    }))
    .sort((a, b) => b.createdAt - a.createdAt);
}

function getFreeKeyItems() {
  if (!freeRuntime || typeof freeRuntime.getKeys !== "function") return [];
  return Object.entries(normalizeAllStore(freeRuntime.getKeys()))
    .map(([key, value]) => ({
      key,
      usesLeft: Number(value.usesLeft || 0),
      usedDevices: Array.isArray(value.devices) ? value.devices.length : 0,
      totalDevices: Number(value.totalDevices || 0),
      expireAt: Number(value.expireAt || 0),
      expireText: formatVNTime(Number(value.expireAt || 0)),
      createdAt: Number(value.createdAt || 0)
    }))
    .sort((a, b) => b.createdAt - a.createdAt);
}

function findKeySummary(key) {
  const inputKey = String(key || "").trim();
  if (!inputKey) return null;
  const record = findKeyRecord(inputKey);
  if (record) {
    const item = record.item;
    return {
      found: true,
      key: inputKey,
      scope: record.scope === "free" ? "free" : "admin",
      status:
        item.expireAt > 0 && item.expireAt <= Date.now()
          ? "expired"
          : item.usesLeft <= 0
            ? "used_up"
            : "active",
      expireAt: Number(item.expireAt || 0),
      expireText: formatVNTime(Number(item.expireAt || 0)),
      usesLeft: Number(item.usesLeft || 0),
      usedDevices: Array.isArray(item.devices) ? item.devices.length : 0,
      totalDevices: Number(item.totalDevices || 0),
      createdAt: Number(item.createdAt || 0)
    };
  }
  if (isExpiredDeletedMainKey(inputKey)) {
    return {
      found: false,
      key: inputKey,
      scope: "admin",
      status: "expired_deleted",
      expireAt: Number(expiredMainKeys[inputKey] || 0),
      expireText: formatVNTime(Number(expiredMainKeys[inputKey] || 0)),
      usesLeft: 0,
      usedDevices: 0,
      totalDevices: 0,
      createdAt: 0
    };
  }
  return {
    found: false,
    key: inputKey,
    scope: "unknown",
    status: "missing",
    expireAt: 0,
    expireText: "--",
    usesLeft: 0,
    usedDevices: 0,
    totalDevices: 0,
    createdAt: 0
  };
}

async function initStore() {
  try {
    if (hasGithubStore()) {
      keys = normalizeAllStore(await readGithubStore());
      expiredMainKeys = normalizeExpiredKeyStore(
        await readGithubStore(MAIN_EXPIRED_GITHUB_DATA_PATH, mainStoreConfig)
      );
      deviceDailyStats = { days: {} };
      cleanupExpiredMainKeys();
      lastSyncedMainKeys = cloneStore(keys);
      console.log("Store ready: GitHub");
    } else {
      keys = normalizeAllStore(loadLocalStore());
      expiredMainKeys = normalizeExpiredKeyStore(loadLocalExpiredMainKeys());
      deviceDailyStats = { days: {} };
      cleanupExpiredMainKeys();
      saveLocalStore();
      saveLocalExpiredMainKeys(expiredMainKeys);
      lastSyncedMainKeys = cloneStore(keys);
      console.log("Store ready: local file");
    }
  } catch (err) {
    console.error("Store init failed, fallback local:", err.message);
    keys = normalizeAllStore(loadLocalStore());
    expiredMainKeys = normalizeExpiredKeyStore(loadLocalExpiredMainKeys());
    deviceDailyStats = { days: {} };
    cleanupExpiredMainKeys();
    saveLocalStore();
    saveLocalExpiredMainKeys(expiredMainKeys);
    lastSyncedMainKeys = cloneStore(keys);
  }
  storeReady = true;
}

async function persistStoreSnapshot(snapshot) {
  try {
    if (hasGithubStore()) {
      const remoteNow = normalizeAllStore(
        await readGithubStore(mainStoreConfig.dataPath, mainStoreConfig)
      );
      const nextSnapshot = applyExternalDeletions(snapshot, remoteNow, lastSyncedMainKeys);
      await writeGithubStore(nextSnapshot, mainStoreConfig.dataPath, "Cập nhật key", mainStoreConfig);
      lastSyncedMainKeys = cloneStore(nextSnapshot);
    } else {
      const localNow = normalizeAllStore(loadLocalStore());
      const nextSnapshot = applyExternalDeletions(snapshot, localNow, lastSyncedMainKeys);
      fs.writeFileSync(STORE_PATH, JSON.stringify(nextSnapshot, null, 2), "utf8");
      lastSyncedMainKeys = cloneStore(nextSnapshot);
    }
  } catch (err) {
    console.error("Persist snapshot failed:", err.message);
    fs.writeFileSync(STORE_PATH, JSON.stringify(snapshot, null, 2), "utf8");
    lastSyncedMainKeys = cloneStore(snapshot);
  }
}

async function saveStore(options = {}) {
  const background = !!options.background;
  cleanupExpiredMainKeys();
  keys = normalizeAllStore(keys);
  const snapshot = JSON.parse(JSON.stringify(keys));
  saveQueue = saveQueue
    .then(() => persistStoreSnapshot(snapshot))
    .catch((err) => {
      console.error("Save store queue failed:", err.message);
    });
  saveExpiredMainKeys({ background: true }).catch(() => {});
  if (!background) return saveQueue;
  return Promise.resolve();
}

app.use((req, res, next) => {
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("Referrer-Policy", "no-referrer");
  res.setHeader("Cache-Control", "no-store");
  next();
});

app.use((req, res, next) => {
  if (!storeReady && !req.path.startsWith("/healthz")) {
    return res.status(503).json({ ok: false, msg: "Store đang khởi động" });
  }
  next();
});

app.use((req, res, next) => {
  const ip =
    (req.headers["x-forwarded-for"] || "")
      .toString()
      .split(",")[0]
      .trim() || req.socket.remoteAddress || "unknown";

  const pathKey = req.path || "/";
  const now = Date.now();
  const limitMap = {
    "/api/check": { windowMs: 60000, limit: 12 },
    "/api/status": { windowMs: 25000, limit: 30 },
    "/api/create": { windowMs: 60000, limit: 20 },
    "/api/delete": { windowMs: 60000, limit: 20 },
    "/api/list": { windowMs: 30000, limit: 40 }
  };
  const activeRule = limitMap[pathKey] || { windowMs: 15000, limit: 90 };
  const bucketKey = ip + "::" + pathKey;

  if (!rateMap.has(bucketKey)) rateMap.set(bucketKey, []);
  const arr = rateMap.get(bucketKey).filter((t) => now - t < activeRule.windowMs);
  arr.push(now);
  rateMap.set(bucketKey, arr);

  if (rateMap.size > RATE_BUCKET_MAX_KEYS || Math.random() < 0.1) {
    for (const [k, values] of rateMap.entries()) {
      const filtered = values.filter((t) => now - t < RATE_BUCKET_TTL_MS);
      if (filtered.length) rateMap.set(k, filtered);
      else rateMap.delete(k);
    }
    if (rateMap.size > RATE_BUCKET_MAX_KEYS) {
      const keys = Array.from(rateMap.keys());
      const overflow = rateMap.size - RATE_BUCKET_MAX_KEYS;
      for (let i = 0; i < overflow; i += 1) {
        rateMap.delete(keys[i]);
      }
    }
  }

  if (arr.length > activeRule.limit) {
    return res.status(429).json({ ok: false, msg: "Thao tác quá nhanh, chờ xíu rồi thử lại" });
  }

  next();
});

function isAdmin(req) {
  const adminKey = String(req.headers["x-admin-key"] || "").trim();
  if (!!ADMIN_KEY && adminKey === ADMIN_KEY) return true;
  return hasAdminSession(req);
}

function genKey() {
  const a = Math.random().toString(36).slice(2, 6).toUpperCase();
  const b = Math.random().toString(36).slice(2, 6).toUpperCase();
  return "ATH-" + a + "-" + b;
}

function isPermanentExpire(ms) {
  return !Number(ms || 0);
}

function formatVNTime(ms) {
  if (isPermanentExpire(ms)) return "Vĩnh viễn";
  return new Date(ms).toLocaleString("vi-VN", { timeZone: VN_TIMEZONE });
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function signText(text) {
  return crypto.createHmac("sha256", SESSION_SECRET).update(text).digest("hex");
}

function parseCookies(req) {
  const out = {};
  String(req.headers.cookie || "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean)
    .forEach((item) => {
      const idx = item.indexOf("=");
      if (idx <= 0) return;
      const key = item.slice(0, idx).trim();
      const value = item.slice(idx + 1).trim();
      out[key] = decodeURIComponent(value);
    });
  return out;
}

function createAdminSessionToken() {
  const issuedAt = Date.now();
  const payload = `admin|${issuedAt}`;
  const sig = signText(payload);
  return Buffer.from(`${payload}|${sig}`, "utf8").toString("base64url");
}

function verifyAdminSessionToken(token) {
  try {
    const raw = Buffer.from(String(token || ""), "base64url").toString("utf8");
    const parts = raw.split("|");
    if (parts.length !== 3) return false;
    const scope = parts[0];
    const issuedAt = Number(parts[1] || 0);
    const sig = parts[2];
    if (scope !== "admin" || !issuedAt || !sig) return false;
    const payload = `${scope}|${issuedAt}`;
    if (sig !== signText(payload)) return false;
    if (issuedAt + ADMIN_SESSION_TTL_MS <= Date.now()) return false;
    return true;
  } catch {
    return false;
  }
}

function hasAdminSession(req) {
  const cookies = parseCookies(req);
  return verifyAdminSessionToken(cookies[ADMIN_SESSION_COOKIE] || "");
}

function setAdminSessionCookie(res) {
  const cookie = [
    `${ADMIN_SESSION_COOKIE}=${encodeURIComponent(createAdminSessionToken())}`,
    `Max-Age=${Math.floor(ADMIN_SESSION_TTL_MS / 1000)}`,
    "Path=/",
    "HttpOnly",
    "SameSite=Lax"
  ];
  if ((process.env.NODE_ENV || "").toLowerCase() === "production" || process.env.RENDER) {
    cookie.push("Secure");
  }
  res.append("Set-Cookie", cookie.join("; "));
}

function clearAdminSessionCookie(res) {
  res.append(
    "Set-Cookie",
    `${ADMIN_SESSION_COOKIE}=; Max-Age=0; Path=/; HttpOnly; SameSite=Lax`
  );
}

function createSessionToken(key, device, expireAt) {
  const issuedAt = Date.now();
  const payload = `${key}|${device}|${expireAt}|${issuedAt}`;
  const sig = signText(payload);
  return Buffer.from(`${payload}|${sig}`, "utf8").toString("base64url");
}

function verifySessionToken(token) {
  try {
    const raw = Buffer.from(token, "base64url").toString("utf8");
    const parts = raw.split("|");
    if (parts.length !== 5) return null;

    const key = parts[0];
    const device = parts[1];
    const expireAt = parts[2];
    const issuedAt = parts[3];
    const sig = parts[4];

    const payload = `${key}|${device}|${expireAt}|${issuedAt}`;
    if (sig !== signText(payload)) return null;

    return {
      key,
      device,
      expireAt: Number(expireAt),
      issuedAt: Number(issuedAt)
    };
  } catch {
    return null;
  }
}

function renderLogo(size, radius) {
  const r = radius || Math.round(size * 0.28);
  if (fs.existsSync(LOGO_PATH)) {
    return `<img src="/logo.png" alt="AimTrickHead Logo" style="width:${size}px;height:${size}px;object-fit:cover;display:block;border-radius:${r}px">`;
  }
  return `<div style="width:${size}px;height:${size}px;border-radius:${r}px;display:flex;align-items:center;justify-content:center;background:linear-gradient(135deg,#8c52ff,#ff70c7);font-size:${Math.round(size * 0.4)}px;color:#fff">ATH</div>`;
}

function iconFacebook(size = 18) {
  return `<img src="https://upload.wikimedia.org/wikipedia/commons/5/51/Facebook_f_logo_%282019%29.svg" alt="Facebook" style="width:${size}px;height:${size}px;display:block;border-radius:999px">`;
}

function iconZalo(size = 20) {
  return `<img src="https://upload.wikimedia.org/wikipedia/commons/9/91/Icon_of_Zalo.svg" alt="Zalo" style="width:${size}px;height:${size}px;display:block;border-radius:8px">`;
}

function iconTikTok(size = 18) {
  return `<img src="https://cdn.simpleicons.org/tiktok/ffffff" alt="TikTok" style="width:${size}px;height:${size}px;display:block;border-radius:4px">`;
}

function iconYouTube(size = 18) {
  return `<img src="https://cdn.simpleicons.org/youtube/ff0000" alt="YouTube" style="width:${size}px;height:${size}px;display:block;border-radius:6px">`;
}

function baseStyles() {
  return `
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Alata&display=swap" rel="stylesheet">
  <style>
    *{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
    html{
      -webkit-text-size-adjust:100%;touch-action:manipulation;
      width:100%;height:100%;min-height:100%;overflow:hidden;overscroll-behavior:none;
      overscroll-behavior-y:none;background:#08090c;position:fixed;inset:0;
    }
    :root{
      --line:rgba(255,255,255,.09);
      --violet:#e7dcff;
      --violet2:#c8b2ff;
      --pink:#8f6cff;
      --muted:#b7adca;
      --ok:#c5e8d2;
      --err:#ffb0c4;
      --gold:#e4cd97;
      --glass:rgba(10,11,14,.92);
      --glass2:rgba(255,255,255,.018);
      --panelEdge:rgba(255,255,255,.07);
      --panelSoft:rgba(255,255,255,.018);
      --shadow:0 36px 120px rgba(0,0,0,.58);
    }
    *{font-family:"Alata", Arial, sans-serif}
    body{
      margin:0;width:100%;height:100%;min-height:100%;color:#fff;overflow:hidden;
      position:fixed;inset:0;overscroll-behavior:none;overscroll-behavior-y:none;isolation:isolate;
      -webkit-overflow-scrolling:auto;background-color:#08090c;
      contain:strict;
      background:
        radial-gradient(circle at 14% 10%, rgba(212,198,160,.07), transparent 24%),
        radial-gradient(circle at 86% 16%, rgba(120,122,132,.045), transparent 22%),
        radial-gradient(circle at 50% 100%, rgba(88,84,70,.09), transparent 32%),
        linear-gradient(160deg,#06070a,#0d0f13,#090b10);
    }
    body:before{
      content:"";position:fixed;inset:0;pointer-events:none;opacity:.22;
      background:linear-gradient(transparent, rgba(255,255,255,.03), transparent);
      background-size:100% 5px;animation:scan 9s linear infinite;
    }
    body:after{
      content:"";position:fixed;inset:-15%;pointer-events:none;opacity:.22;
      background:
        radial-gradient(circle at 20% 20%, rgba(255,255,255,.06) 1px, transparent 1.5px),
        radial-gradient(circle at 80% 70%, rgba(255,255,255,.05) 1px, transparent 1.6px);
      background-size:22px 22px, 28px 28px;
      animation:moveDots 22s linear infinite;
    }
    .bgAura{
      position:fixed;inset:0;pointer-events:none;overflow:hidden;z-index:0;
    }
    .orb{
      position:absolute;border-radius:50%;filter:blur(18px);opacity:.26;animation:floatOrb 16s ease-in-out infinite;
    }
    .orb.o1{width:180px;height:180px;left:-40px;top:12%;background:rgba(205,186,138,.12)}
    .orb.o2{width:220px;height:220px;right:-60px;top:30%;background:rgba(136,140,152,.10);animation-delay:-6s}
    .orb.o3{width:200px;height:200px;left:30%;bottom:-90px;background:rgba(170,148,102,.08);animation-delay:-10s}
    @keyframes scan{from{transform:translateY(-100%)}to{transform:translateY(100%)}}
    @keyframes moveDots{from{transform:translateY(0)}to{transform:translateY(80px)}}
    @keyframes glow{
      0%{box-shadow:0 0 18px rgba(209,187,138,.10),0 0 36px rgba(255,255,255,.025)}
      50%{box-shadow:0 0 34px rgba(209,187,138,.16),0 0 68px rgba(255,255,255,.045)}
      100%{box-shadow:0 0 18px rgba(209,187,138,.10),0 0 36px rgba(255,255,255,.025)}
    }
    @keyframes pulseText{
      0%{text-shadow:0 0 10px rgba(209,187,138,.12)}
      50%{text-shadow:0 0 18px rgba(255,255,255,.10)}
      100%{text-shadow:0 0 10px rgba(209,187,138,.12)}
    }
    @keyframes neonBar{
      0%{background-position:0% 50%}
      100%{background-position:200% 50%}
    }
    @keyframes popIn{
      0%{opacity:0;transform:scale(.96)}
      100%{opacity:1;transform:scale(1)}
    }
    @keyframes floatOrb{
      0%,100%{transform:translate3d(0,0,0)}
      50%{transform:translate3d(18px,-24px,0)}
    }
    .wrap{
      position:relative;z-index:1;
      width:100%;height:100vh;height:100dvh;min-height:100vh;min-height:100dvh;display:flex;align-items:center;justify-content:center;
      padding:max(12px,env(safe-area-inset-top)) max(12px,env(safe-area-inset-right)) max(12px,env(safe-area-inset-bottom)) max(12px,env(safe-area-inset-left));
      overflow:hidden;background:transparent;
    }
      .card{
        width:min(94vw,560px);height:min(calc(100dvh - 12px),980px);max-height:calc(100dvh - 12px);overflow:auto;
        contain:layout paint style;
        border-radius:32px;background:linear-gradient(180deg, rgba(15,16,19,.94), rgba(8,9,12,.96));
        border:1px solid var(--panelEdge);animation:glow 10s infinite;
      backdrop-filter:blur(20px) saturate(1.01);
      box-shadow:var(--shadow), inset 0 1px 0 rgba(255,255,255,.04), inset 0 -1px 0 rgba(255,255,255,.02);
        overscroll-behavior:contain;-webkit-overflow-scrolling:touch;
        position:relative;
      }
      .card::-webkit-scrollbar{width:0;height:0}
      .cardSnowLayer{
        position:absolute;inset:0;pointer-events:none;overflow:hidden;z-index:1;opacity:0;
        transition:opacity .35s ease;
      }
      .cardSnowLayer.show{opacity:1}
      .cardSnowFlake{
        position:absolute;top:-12vh;border-radius:50%;
        background:radial-gradient(circle at 35% 35%, rgba(255,255,255,.94), rgba(255,255,255,.22) 70%, transparent 72%);
        filter:drop-shadow(0 0 8px rgba(255,255,255,.12));
        animation-name:iosSnowFall;
        animation-timing-function:linear;
        animation-iteration-count:infinite;
        will-change:transform, opacity;
      }
      .top{
        padding:22px 20px 18px;border-bottom:1px solid rgba(255,255,255,.05);
        position:sticky;top:0;z-index:8;overflow:hidden;backdrop-filter:blur(20px) saturate(120%);
        background:linear-gradient(180deg,rgba(14,15,18,.985),rgba(14,15,18,.90))
      }
    .top::before{
      content:"";position:absolute;inset:auto -10% -60% auto;width:280px;height:280px;
      background:radial-gradient(circle, rgba(168,132,255,.14), transparent 65%);
      pointer-events:none
    }
    .top::after{
      content:"";position:absolute;left:-20%;top:-1px;width:140%;height:4px;
      background:linear-gradient(90deg,transparent,rgba(255,255,255,.06),rgba(201,176,122,.22),transparent);
      background-size:200% 100%;animation:neonBar 4.8s linear infinite
    }
    .brand{display:flex;align-items:center;gap:18px}
    .logoBox{
      width:88px;height:88px;border-radius:26px;overflow:hidden;
      box-shadow:0 18px 44px rgba(0,0,0,.30), inset 0 1px 0 rgba(255,255,255,.06), 0 0 28px rgba(168,132,255,.12);flex:0 0 88px;
      background:rgba(255,255,255,.032)
    }
    .top .logoBox{
      position:relative;
      animation:logoFloatGlow 6.8s ease-in-out infinite;
    }
    .title{margin:0;font-size:clamp(29px,6vw,40px);line-height:1.04;color:#efe8ff;letter-spacing:.2px;animation:pulseText 7s infinite;font-weight:700;text-shadow:0 0 24px rgba(168,132,255,.16),0 0 40px rgba(255,255,255,.05)}
    .sub{margin:8px 0 0;color:#d7cde8;font-size:14px}
    .credit{margin-top:12px;color:var(--gold);font-size:12px;font-weight:700;letter-spacing:1.1px;text-transform:uppercase;opacity:.96}
      .content{padding:24px 18px 20px;flex:1;overflow:auto;-webkit-overflow-scrolling:touch;overscroll-behavior:contain;scrollbar-width:none;position:relative;z-index:2}
    .content::-webkit-scrollbar{width:0;height:0}
    .loginStrip{display:flex;gap:8px;flex-wrap:wrap;margin:0 0 14px;position:relative;z-index:1}
    .loginStrip span,.statChip{border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.04);box-shadow:inset 0 1px 0 rgba(255,255,255,.06);}
    .loginStrip span{padding:7px 10px;border-radius:999px;font-size:11px;color:#e9e1d2;letter-spacing:.92px;text-transform:uppercase}
    .quickStats{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin:12px 0 14px}
    .statChip{padding:11px 12px;border-radius:18px;backdrop-filter:blur(10px)}
    .statLabel{display:block;font-size:11px;color:#beaed8;margin-bottom:4px}
    .statChip strong{font-size:14px;color:#fff}
    .loginHero{
      position:relative;
      padding:clamp(20px,3.6vw,34px) 0 clamp(12px,2vw,18px);border-radius:0;
      background:transparent;
      border:none;
      overflow:visible;
      box-shadow:none;
      animation:none;
      display:grid;gap:clamp(14px,2.2vw,20px);
      align-items:start;
    }
    .loginHero:before{
      display:none;
    }
    .loginHero:after{
      display:none;
    }
    .loginIntro{position:relative;z-index:1;margin-bottom:clamp(18px,3vw,26px)}
    .loginEyebrow{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      min-height:28px;
      padding:0 12px;
      margin-bottom:14px;
      border-radius:999px;
      border:1px solid rgba(255,236,185,.14);
      background:linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,.025));
      color:#e9d6a5;
      font-size:11px;
      font-weight:700;
      letter-spacing:1.12px;
      text-transform:uppercase;
      box-shadow:inset 0 1px 0 rgba(255,255,255,.04), 0 8px 18px rgba(0,0,0,.10);
    }
    .loginTitle{font-size:clamp(30px,5.2vw,38px);color:#fff7db;margin:0 0 10px;letter-spacing:.42px;text-shadow:0 0 22px rgba(255,224,130,.24),0 0 46px rgba(255,255,255,.09)}
    .loginHint{margin:0 auto;color:#ddd2b2;font-size:clamp(14px,2.05vw,16px);line-height:1.78;max-width:31rem;overflow-wrap:anywhere;word-break:break-word}
    .loginAccent{
      width:min(104px,42%);
      height:1px;
      margin:16px auto 0;
      background:linear-gradient(90deg, transparent, rgba(228,205,151,.95), rgba(168,132,255,.48), transparent);
      box-shadow:0 0 14px rgba(228,205,151,.16);
    }

    .loginFeatureList{
      display:flex;flex-direction:column;gap:10px;
      margin:14px 0 6px;
      padding:0 2px;
      width:min(100%,560px);
      align-self:center;
    }
    .loginFeatureItem{
      display:flex;align-items:center;justify-content:flex-start;gap:10px;
      text-align:left;
      color:#dfffe9;
      font-size:14px;
      line-height:1.55;
      letter-spacing:.15px;
      width:100%;
    }
    .loginFeatureTick{
      width:20px;height:20px;min-width:20px;border-radius:999px;
      display:inline-flex;align-items:center;justify-content:center;
      background:linear-gradient(180deg, rgba(46,211,122,.28), rgba(46,211,122,.14));
      border:1px solid rgba(73,255,154,.36);
      box-shadow:0 0 16px rgba(57,225,136,.18);
      color:#7dffba;
      font-size:12px;
      transform:translateY(-1px);
    }
    .loginFrame{
      position:relative;padding:clamp(24px,3.8vw,34px) clamp(16px,3vw,24px) clamp(16px,2.4vw,20px);display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;gap:0;
      width:min(100%,560px);min-height:auto;margin:0 auto;animation:none;
    }
    .loginFrame::before{
      display:none
    }
    .loginFrame::after{
      display:none
    }
    .loginStrip{
      display:flex;gap:10px;flex-wrap:wrap;margin-top:4px
    }
    .loginChip{
      display:inline-flex;align-items:center;gap:8px;padding:10px 12px;border-radius:999px;
      background:linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.022));
      border:1px solid rgba(255,255,255,.08);font-size:12px;color:#f1ece3;
      box-shadow:inset 0 1px 0 rgba(255,255,255,.03), 0 8px 18px rgba(0,0,0,.10);
    }
    .input{
      width:100%;height:clamp(56px,7.2vh,64px);border:none;outline:none;border-radius:22px;padding:0 clamp(16px,2.8vw,20px);
      color:#fff;background:linear-gradient(180deg, rgba(255,255,255,.072), rgba(255,255,255,.024));border:1px solid rgba(255,235,172,.17);font-size:clamp(15px,2vw,16px);
      box-shadow:inset 0 1px 0 rgba(255,255,255,.04), 0 16px 32px rgba(0,0,0,.16)
    }
    .input::placeholder{
      color:#bcb2c8;
    }
    .input:focus{
      border-color:rgba(255,220,120,.50);box-shadow:0 0 0 3px rgba(255,220,120,.08), 0 14px 28px rgba(0,0,0,.16)
    }
    #loginView .btn,
    #loginView .socialBtn,
    #loginView .loginIconLink{
      transition:none;
      animation:none;
    }
    #loginView .btn:before,
    #loginView .socialBtn:before{
      display:none;
    }
    .btn,.smallBtn,.tab,.gameBtn{
      border:none;color:#fff;cursor:pointer;font-weight:700;border-radius:16px
    }
    .btn{
      width:auto;min-width:clamp(148px,22vw,176px);height:clamp(50px,6.4vh,56px);margin-top:16px;padding:0 clamp(22px,3vw,28px);
      background:linear-gradient(180deg, rgba(255,255,255,.10), rgba(255,255,255,.035));
      border:1px solid rgba(255,239,176,.26);
      box-shadow:0 14px 28px rgba(0,0,0,.18), inset 0 1px 0 rgba(255,255,255,.08), 0 0 22px rgba(228,205,151,.06);
      letter-spacing:.18px;
      backdrop-filter:blur(10px)
    }
    #loginView .btn{
      min-width:min(220px,100%);
    }
    .btn:hover,.gameBtn:hover,.smallBtn:hover{transform:translateY(-1px)}
    .smallBtn{
      height:38px;padding:0 12px;background:rgba(255,255,255,.08);border:1px solid var(--line)
    }
    .msg{min-height:22px;margin-top:12px;text-align:center;font-size:14px;line-height:1.6;overflow-wrap:anywhere;word-break:break-word}
    .ok{color:var(--ok)}
    .err{color:var(--err)}
    .hidden{display:none!important}
    .topLine{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:14px}
    .pill{
      display:inline-flex;align-items:center;gap:8px;padding:10px 12px;border-radius:999px;
      background:linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.03));border:1px solid var(--line);font-size:12px;color:#efe5cf;box-shadow:inset 0 1px 0 rgba(255,255,255,.04)
    }
    .noticeBox{
      margin-top:12px;padding:13px 14px;border-radius:16px;background:linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,.03));
      border:1px solid var(--line);font-size:13px;color:#efe7ff;line-height:1.6;overflow-wrap:anywhere;word-break:break-word
    }
      .tabs{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:22px 0 18px}
    .tab{height:56px;border-radius:18px;border:1px solid var(--line);background:rgba(255,255,255,.05);font-size:14px;padding:0 10px}
    .tab.active{background:linear-gradient(90deg,#a684ff,#e4cd97,#f3ebdf)}
    .tabPane{display:none}
    .tabPane.active{display:block}
    .tile{
      padding:16px;border-radius:24px;margin-bottom:12px;
      background:linear-gradient(180deg, rgba(255,255,255,.040), rgba(255,255,255,.016));
      border:1px solid rgba(255,255,255,.065);position:relative;overflow:hidden;
      transition:transform .24s ease, box-shadow .24s ease, border-color .24s ease;
      box-shadow:inset 0 1px 0 rgba(255,255,255,.02)
    }
    .tile:hover{transform:translateY(-2px);box-shadow:0 18px 36px rgba(0,0,0,.20);border-color:rgba(201,176,122,.14)}
    .tile::before{
      content:"";position:absolute;width:140px;height:140px;right:-40px;bottom:-40px;
      background:radial-gradient(circle, rgba(201,176,122,.07), transparent 65%)
    }
    .row{display:flex;align-items:center;justify-content:space-between;gap:12px;position:relative;z-index:1}
    .name{margin:0;font-size:16px;overflow-wrap:anywhere;word-break:break-word}
    .desc{margin:6px 0 0;color:#c1b9d4;font-size:12px;line-height:1.5;overflow-wrap:anywhere;word-break:break-word}
    .switch{position:relative;width:62px;height:34px;flex:0 0 62px}
    .switch input{display:none}
    .slider{
      position:absolute;inset:0;border-radius:999px;background:linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,.025));
      border:1px solid rgba(255,234,171,.12);transition:.28s;cursor:pointer;
      box-shadow:inset 0 1px 0 rgba(255,255,255,.05), 0 6px 14px rgba(0,0,0,.14)
    }
    .slider:before{
      content:"";position:absolute;width:26px;height:26px;left:4px;top:3px;border-radius:50%;
      background:linear-gradient(180deg,#fff8de,#fff);transition:.28s;
      box-shadow:0 3px 12px rgba(255,229,138,.16)
    }
    .slider:after{
      content:"";position:absolute;inset:0;border-radius:999px;opacity:0;transition:.28s;
      background:linear-gradient(90deg, rgba(255,214,92,.18), rgba(255,240,180,.28));
      box-shadow:0 0 18px rgba(255,214,92,.16)
    }
    .switch input:checked + .slider{
      background:linear-gradient(90deg,rgba(24,198,94,.70),rgba(121,255,166,.90));border-color:rgba(118,255,172,.42);box-shadow:0 0 0 1px rgba(118,255,172,.14), 0 0 26px rgba(44,228,116,.24)
    }
    .switch input:checked + .slider:before{transform:translateX(28px);box-shadow:0 6px 18px rgba(98,255,155,.26)}
    .switch input:checked + .slider:after{opacity:1}
    .grid2{display:grid;grid-template-columns:1fr 1fr;gap:10px}
    .socialBtn,.gameBtn{
      display:flex;align-items:center;justify-content:center;gap:10px;height:52px;border-radius:16px;
      text-decoration:none;color:#fff;background:rgba(255,255,255,.07);border:1px solid var(--line);font-weight:700
    }
    .loginIconRow{display:flex;align-items:center;justify-content:center;gap:clamp(34px,8vw,68px);margin-top:18px}
    .loginIconLink{display:inline-flex;align-items:center;justify-content:center;text-decoration:none;transition:transform .24s ease, filter .24s ease}
    .loginIconLink:hover{transform:translateY(-2px);filter:drop-shadow(0 0 18px rgba(255,224,130,.20))}
    @keyframes floatPanel{
      0%,100%{transform:translateY(0) scale(1)}
      50%{transform:translateY(-10px) scale(1.01)}
    }
    .gameBtn{
      background:linear-gradient(90deg,rgba(108,112,122,.24),rgba(201,176,122,.16));
      box-shadow:0 10px 22px rgba(183,124,255,.12)
    }
    .socialBtn:hover,.gameBtn:hover{box-shadow:0 0 18px rgba(201,176,122,.10)}
    .footer{margin-top:10px;text-align:center;font-size:12px;color:#b9b0c9;line-height:1.7;overflow-wrap:anywhere;word-break:break-word}
      .liveFx{
        margin-top:14px;padding:82px 16px 16px;border-radius:24px;
        background:
          linear-gradient(180deg, rgba(7,12,24,.96), rgba(15,10,31,.90)),
          radial-gradient(circle at top right, rgba(97,180,255,.08), transparent 28%);
        border:1px solid rgba(160,132,255,.18);color:#f1e8ff;font-size:12px;min-height:186px;
        white-space:pre-wrap;line-height:1.7;position:relative;overflow:hidden;
        box-shadow:inset 0 0 0 1px rgba(255,255,255,.03), 0 18px 40px rgba(4,0,18,.28);
        font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;
      }
    .liveFx::before{
      content:"";position:absolute;inset:0;background:linear-gradient(180deg, transparent, rgba(255,255,255,.03), transparent);
      background-size:100% 90px;animation:scan 7s linear infinite;pointer-events:none;opacity:.46;
    }
    .liveFx::after{
      content:"";position:absolute;left:-20%;top:32px;width:60%;height:1px;background:linear-gradient(90deg, transparent, rgba(220,225,255,.58), transparent);
      box-shadow:0 0 18px rgba(220,225,255,.16);animation:neonBar 4.1s linear infinite;pointer-events:none;
    }
      .liveFxHud{
        position:absolute;left:14px;right:14px;top:10px;display:flex;align-items:center;justify-content:space-between;
        font-size:10px;color:#9bc6ff;letter-spacing:1px;text-transform:uppercase;opacity:.92
      }
      .liveFxControls{
        position:absolute;left:14px;right:14px;top:38px;display:flex;justify-content:flex-end;gap:8px;z-index:2;
      }
      .monitorGameBtn{
        min-width:64px;height:28px;padding:0 12px;border-radius:999px;border:1px solid rgba(255,255,255,.10);
        background:linear-gradient(180deg, rgba(255,255,255,.10), rgba(255,255,255,.04));
        color:#f8f1df;font-size:11px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;cursor:pointer;
        box-shadow:inset 0 1px 0 rgba(255,255,255,.04), 0 8px 18px rgba(0,0,0,.14);
      }
      .monitorGameBtn:hover{
        border-color:rgba(214,196,154,.26);
        background:linear-gradient(180deg, rgba(181,152,255,.18), rgba(228,205,151,.12));
      }
    .hudDots{display:flex;gap:6px;align-items:center}
    .hudDots span{width:8px;height:8px;border-radius:50%;display:block;background:rgba(255,255,255,.16)}
    .hudDots span:nth-child(1){background:#c8c2d6;box-shadow:0 0 10px rgba(200,194,214,.28)}
    .hudDots span:nth-child(2){background:#d6c49a;box-shadow:0 0 10px rgba(214,196,154,.24)}
    .hudDots span:nth-child(3){background:#ece7d7;box-shadow:0 0 10px rgba(236,231,215,.20)}

    .liveFx.activePulse{
      border-color:rgba(123,221,255,.45);
      box-shadow:inset 0 0 0 1px rgba(255,255,255,.04), 0 0 0 1px rgba(122,206,255,.10), 0 18px 40px rgba(4,0,18,.32), 0 0 30px rgba(98,178,255,.16);
    }
    .fxLine{display:block;opacity:0;transform:translateY(6px);animation:fxLineIn .34s ease forwards}
    .fxCursor{
      display:inline-block;width:9px;height:15px;margin-left:6px;vertical-align:-2px;border-radius:2px;
      background:linear-gradient(180deg,#8ad0ff,#ffffff);box-shadow:0 0 10px rgba(138,208,255,.45);
      animation:blinkCursor .9s steps(1) infinite;
    }
    .fxLine.ok{color:#aef7d1}.fxLine.warn{color:#ffd98b}.fxLine.info{color:#b9e0ff}
    .fxLine:nth-child(2){animation-delay:.06s}
    .fxLine:nth-child(3){animation-delay:.12s}
    .fxLine:nth-child(4){animation-delay:.18s}
    .fxLine:nth-child(5){animation-delay:.24s}
    .fxLine:nth-child(6){animation-delay:.30s}
    .sliderWrap{margin-top:10px}
    .rangeLabel{
      display:flex;align-items:center;justify-content:space-between;font-size:12px;color:#e5dcf5;margin-bottom:8px
    }
    input[type=range]{width:100%;accent-color:#c86bff}
    .toast{
      position:fixed;left:50%;bottom:18px;transform:translateX(-50%) translateY(20px);
      min-width:220px;max-width:92vw;padding:14px 16px;border-radius:16px;background:rgba(12,15,24,.95);
      border:1px solid var(--line);color:#fff;text-align:center;z-index:120;opacity:0;pointer-events:none;
      transition:.25s
    }
    .toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
    .toast.ok{color:var(--ok)}
    .toast.err{color:var(--err)}
    .loadingLayer{
      position:fixed;inset:0;z-index:9999;
      display:flex;align-items:center;justify-content:center;flex-direction:column;
      background:
        radial-gradient(circle at center, rgba(170,90,255,.18), transparent 30%),
        linear-gradient(160deg,#030207,#0b0612,#05040b);
      transition:opacity .55s ease, visibility .55s ease;
    }
    .loadingLayer.hide{opacity:0;visibility:hidden}
    .loadingLogo{
      width:172px;height:172px;border-radius:28px;overflow:hidden;
      box-shadow:0 0 30px rgba(209,187,138,.16),0 0 70px rgba(255,111,216,.12);
      animation:glow 3s infinite, popIn .7s ease;
      background:rgba(255,255,255,.028);position:relative
    }
    .loadingLogo::after{
      content:"";position:absolute;inset:0;border-radius:28px;border:1px solid rgba(255,255,255,.09)
    }
    .loadingText{
      margin-top:18px;font-size:16px;color:var(--violet);font-weight:800;letter-spacing:1px;
      animation:pulseText 2s infinite;
    }
    .loadingSub{margin-top:8px;color:#cfc7b6;font-size:12px;letter-spacing:.5px}
    .loadingBar{
      width:min(260px,72vw);height:8px;border-radius:999px;margin-top:16px;background:rgba(255,255,255,.08);overflow:hidden;border:1px solid rgba(255,255,255,.08)
    }
    .loadingBar > span{
      display:block;height:100%;width:35%;
      background:linear-gradient(90deg,#a684ff,#e4cd97,#f4ecde);
      border-radius:999px;animation:neonBar 1.2s linear infinite;background-size:200% 100%
    }

    .fxOverlay{
      position:fixed;inset:0;z-index:10000;display:flex;align-items:center;justify-content:center;
      background:rgba(3,8,20,.62);backdrop-filter:blur(10px);padding:24px;
      transition:opacity .25s ease, visibility .25s ease
    }
    .fxOverlay.hidden{opacity:0;visibility:hidden;pointer-events:none}
    .fxGlow{
      position:absolute;width:180px;height:180px;border-radius:50%;
      background:radial-gradient(circle, rgba(255,255,255,.92) 0%, rgba(215,214,235,.38) 24%, rgba(130,132,160,.12) 50%, transparent 70%);
      filter:blur(6px);top:22%;left:50%;transform:translateX(-50%);pointer-events:none
    }
    .fxOverlayCard{
      position:relative;z-index:1;width:min(680px,92vw);min-height:300px;border-radius:30px;
      display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;
      background:linear-gradient(180deg, rgba(18,18,24,.88), rgba(10,10,16,.92));
      border:1px solid rgba(255,255,255,.10);box-shadow:0 24px 72px rgba(0,0,0,.42), inset 0 1px 0 rgba(255,255,255,.03);
      overflow:hidden;padding:32px
    }
    .fxOverlayCard::before,.fxOverlayCard::after{content:"";position:absolute;border-radius:50%;background:rgba(82,123,255,.10);filter:blur(18px)}
    .fxOverlayCard::before{width:140px;height:140px;left:12%;top:18%}
    .fxOverlayCard::after{width:120px;height:120px;right:12%;bottom:18%}
    .fxOverlayTitle{font-size:clamp(36px,9vw,58px);font-weight:800;letter-spacing:1px}
    .fxOverlaySub{margin-top:12px;font-size:clamp(16px,3.5vw,22px);color:#d6e0ff;overflow-wrap:anywhere;word-break:break-word}
    .prepCard{min-height:320px}
    .prepSpinner{width:82px;height:82px;border-radius:50%;border:4px solid rgba(255,255,255,.16);border-top-color:#fff;animation:spin .9s linear infinite;margin-bottom:18px}
    .prepBar{width:min(320px,72vw);height:10px;border-radius:999px;margin-top:18px;background:rgba(255,255,255,.08);overflow:hidden;border:1px solid rgba(255,255,255,.09)}
    .prepBar span{display:block;height:100%;width:45%;background:linear-gradient(90deg,#8bb4ff,#ffffff,#8bb4ff);background-size:200% 100%;animation:neonBar 1s linear infinite}
    .homeScreen{width:min(1100px,96vw);height:min(calc(100dvh - 24px),980px);min-height:calc(100dvh - 24px);display:flex;align-items:stretch;position:relative}
    .homeGlass{flex:1;border-radius:32px;background:linear-gradient(180deg, rgba(8,14,34,.58), rgba(8,14,34,.40));border:1px solid rgba(255,255,255,.10);backdrop-filter:blur(18px);box-shadow:0 24px 90px rgba(0,0,0,.35);padding:28px;display:flex;flex-direction:column;justify-content:space-between;overflow:auto;overscroll-behavior:contain;-webkit-overflow-scrolling:touch;position:relative;contain:layout paint style;background-clip:padding-box}
    .homeGlass::before{content:"";position:absolute;inset:auto -10% 55% auto;width:360px;height:360px;border-radius:50%;background:radial-gradient(circle, rgba(126,168,255,.22), transparent 68%);pointer-events:none}
    .heroRow{display:flex;justify-content:space-between;align-items:flex-start;gap:20px;flex-wrap:wrap}
    .heroIntro{max-width:620px}
    .heroTitle{margin:0;font-size:clamp(34px,7vw,68px);line-height:1.02}
    .heroNote{margin-top:14px;color:#cfd8ef;font-size:15px;max-width:540px;line-height:1.7}
    .loginPanel{margin-top:24px;width:min(560px,100%);padding:0;border-radius:0;background:transparent;border:none;box-shadow:none}

    .tabs{position:sticky;top:0;z-index:2;padding-top:2px;background:linear-gradient(180deg,rgba(11,8,18,.95),rgba(11,8,18,.72) 70%,transparent)}
    .tab{backdrop-filter:blur(10px)}
    .tab.active{box-shadow:0 0 0 1px rgba(255,255,255,.08),0 10px 28px rgba(183,124,255,.18)}
    .tile{position:relative;overflow:hidden}
    .tile:before{content:"";position:absolute;inset:0 auto 0 -30%;width:32%;background:linear-gradient(90deg,transparent,rgba(255,255,255,.05),transparent);transform:skewX(-18deg);animation:tileSweep 6.8s linear infinite;pointer-events:none}
    .liveFx{position:sticky;bottom:0;z-index:2;background:linear-gradient(180deg,rgba(10,8,18,.35),rgba(10,8,18,.96) 16%,rgba(10,8,18,.98));padding-bottom:calc(10px + env(safe-area-inset-bottom));margin-bottom:-6px}
      .liveFxHud{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}
      .fxMeta{display:flex;align-items:center;gap:8px;font-size:10px;color:#d6c8aa;letter-spacing:.9px;text-transform:uppercase}
    .fxMeter{width:54px;height:6px;border-radius:999px;background:rgba(255,255,255,.08);overflow:hidden;position:relative}
    .fxMeter:before{content:"";position:absolute;inset:0;width:72%;background:linear-gradient(90deg,#bea6ff,#e4cd97);animation:meterRun 2s linear infinite}
    .btn,.gameBtn,.socialBtn,.smallBtn{position:relative;overflow:hidden}
    .btn:before,.gameBtn:before,.socialBtn:before,.smallBtn:before{content:"";position:absolute;inset:0 auto 0 -42%;width:34%;background:linear-gradient(90deg,transparent,rgba(255,255,255,.18),transparent);transform:skewX(-20deg)}
    .btn:hover:before,.gameBtn:hover:before,.socialBtn:hover:before,.smallBtn:hover:before{animation:btnSweep .9s ease}
    @keyframes tileSweep{0%{transform:translateX(0) skewX(-18deg);opacity:0}10%{opacity:1}100%{transform:translateX(520%) skewX(-18deg);opacity:0}}
    @keyframes meterRun{0%{transform:translateX(-40%)}100%{transform:translateX(55%)}}
    @keyframes btnSweep{0%{transform:translateX(0) skewX(-20deg);opacity:0}20%{opacity:1}100%{transform:translateX(520%) skewX(-20deg);opacity:0}}

    
    .card::before{
      content:"";position:absolute;inset:0;border-radius:32px;padding:1px;
      background:linear-gradient(135deg, rgba(255,255,255,.10), rgba(255,255,255,.025) 22%, rgba(214,196,154,.16) 56%, rgba(255,255,255,.03) 88%);
      -webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);
      -webkit-mask-composite:xor;mask-composite:exclude;pointer-events:none;opacity:.9;
    }
    .luxBar{
      background:linear-gradient(90deg, rgba(255,255,255,.045), rgba(214,196,154,.08), rgba(255,255,255,.03));
      border:1px solid rgba(255,255,255,.07);
      box-shadow:inset 0 1px 0 rgba(255,255,255,.04);
    }
    .pill{
      background:linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.028));
      border:1px solid rgba(255,255,255,.08);
    }
    .tile,.noticeBox,.statChip,.socialBtn,.gameBtn,.tab{
      box-shadow:inset 0 1px 0 rgba(255,255,255,.035), 0 12px 28px rgba(0,0,0,.12);
    }
    .liveFx{
      border-top:1px solid rgba(255,255,255,.06);
      box-shadow:0 -16px 44px rgba(0,0,0,.18);
    }
    .liveFx::before{
      content:"";position:absolute;left:16px;right:16px;top:0;height:1px;
      background:linear-gradient(90deg, transparent, rgba(214,196,154,.30), transparent);
      pointer-events:none;
    }
    .fxMeter:before{background:linear-gradient(90deg,#cdb8ff,#e4cd97,#f5eee4)}
    .loadingText,.title{font-weight:400}
    .statusBatteryRemoved{display:none!important}
    .statusLabel{display:none!important}
    .statusTime:empty{display:none!important}

    .btn,.smallBtn,.socialBtn,.gameBtn,.tab{transition:transform .18s ease, box-shadow .18s ease, background .18s ease, border-color .18s ease}
    .btn:hover,.smallBtn:hover,.socialBtn:hover,.gameBtn:hover,.tab:hover{transform:translateY(-1px);box-shadow:0 10px 24px rgba(0,0,0,.18)}
    .btn:active,.smallBtn:active,.socialBtn:active,.gameBtn:active,.tab:active{transform:scale(.985)}
    @keyframes spin{to{transform:rotate(360deg)}}
    @keyframes fxLineIn{
      0%{opacity:0;transform:translateY(6px)}
      100%{opacity:1;transform:translateY(0)}
    }
    @keyframes blinkCursor{
      0%,49%{opacity:1}
      50%,100%{opacity:.15}
    }
    @keyframes panelLiftIn{
      from{opacity:0;transform:translateY(12px) scale(.988)}
      to{opacity:1;transform:translateY(0) scale(1)}
    }
    @keyframes logoFloatGlow{
      0%,100%{
        transform:translateY(0);
        box-shadow:0 18px 44px rgba(0,0,0,.30), inset 0 1px 0 rgba(255,255,255,.06), 0 0 28px rgba(168,132,255,.12);
      }
      50%{
        transform:translateY(-4px);
        box-shadow:0 20px 48px rgba(0,0,0,.32), inset 0 1px 0 rgba(255,255,255,.06), 0 0 36px rgba(214,196,154,.14);
      }
    }
    @media (prefers-reduced-motion: reduce){
      .top .logoBox{
        animation:none !important;
      }
    }
    #panelView.panelReveal .topLine,
    #panelView.panelReveal .quickStats,
    #panelView.panelReveal #keyNotice,
    #panelView.panelReveal .tabs,
    #panelView.panelReveal .tabPane.active,
    #panelView.panelReveal #liveFxBox{
      animation:panelLiftIn .42s ease both;
      will-change:transform,opacity;
    }
    #panelView.panelReveal .quickStats{animation-delay:.04s}
    #panelView.panelReveal #keyNotice{animation-delay:.08s}
    #panelView.panelReveal .tabs{animation-delay:.12s}
    #panelView.panelReveal .tabPane.active{animation-delay:.16s}
    #panelView.panelReveal #liveFxBox{animation-delay:.2s}

    @media (max-width:560px){
        .tabs{grid-template-columns:repeat(2,1fr)}
        .grid2{grid-template-columns:1fr}
      .wrap{padding:0}
      .card{
        width:100vw;max-width:100vw;height:100dvh;max-height:100dvh;
        border-radius:0;border-left:none;border-right:none;border-top:none
      }
      .homeScreen{width:100vw;height:100dvh;min-height:100dvh}
      .homeGlass{border-radius:0;padding:20px 16px 18px;border-left:none;border-right:none;border-top:none}
      .brand{align-items:center;gap:14px}
      .top{padding-top:calc(18px + env(safe-area-inset-top));padding-bottom:18px}
      .logoBox{width:84px;height:84px;flex-basis:84px;border-radius:24px}
      .title{font-size:clamp(28px,8vw,36px)}
      .sub{font-size:13px}
      .credit{font-size:11px}
        .content{padding:16px 14px calc(20px + env(safe-area-inset-bottom))}
        .loginHero{padding:22px 0 10px}
        .loginFrame{width:min(100%,100%);min-height:auto;padding:22px 12px 12px}
        .loginFrame::before{left:-8px;right:-8px;top:-10px;bottom:-10px;border-radius:28px}
        .loginFrame::after{left:-14px;right:-14px;top:-18px;bottom:-18px}
        .loginTitle{font-size:30px}
        .loginHint{font-size:13px;max-width:92%}
        .liveFx{padding-top:98px;min-height:208px}
        .liveFxControls{justify-content:flex-start;top:44px;flex-wrap:wrap}
        .monitorGameBtn{min-width:72px}
        .toast{bottom:calc(14px + env(safe-area-inset-bottom))}
      }
  
    html.non-ios-scroll-fix{
      position:relative !important;
      inset:auto !important;
      overflow-x:hidden !important;
      overflow-y:auto !important;
      height:auto !important;
      min-height:100% !important;
      overscroll-behavior:auto !important;
    }
    html.non-ios-scroll-fix body{
      position:relative !important;
      inset:auto !important;
      overflow-x:hidden !important;
      overflow-y:auto !important;
      height:auto !important;
      min-height:100dvh !important;
      contain:none !important;
      -webkit-overflow-scrolling:touch !important;
      overscroll-behavior:auto !important;
    }
    html.non-ios-scroll-fix .wrap{
      height:auto !important;
      min-height:100dvh !important;
      overflow:visible !important;
      align-items:flex-start !important;
      justify-content:center !important;
    }
    html.non-ios-scroll-fix .card,
    html.non-ios-scroll-fix .homeScreen,
    html.non-ios-scroll-fix .homeGlass,
    html.non-ios-scroll-fix .content,
    html.non-ios-scroll-fix .tabPane,
    html.non-ios-scroll-fix .tabPane.active,
    html.non-ios-scroll-fix .tabContent,
    html.non-ios-scroll-fix .tabsWrap,
    html.non-ios-scroll-fix .panelBody,
    html.non-ios-scroll-fix #list{
      position:relative !important;
      max-height:none !important;
      height:auto !important;
      min-height:0 !important;
      overflow-x:hidden !important;
      overflow-y:auto !important;
      contain:none !important;
      touch-action:pan-y !important;
      -webkit-overflow-scrolling:touch !important;
      overscroll-behavior:auto !important;
    }

  </style>
  `;
}

function renderHomeHtml() {
  return `
<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">
  <meta name="theme-color" content="#05040b">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
  <meta name="mobile-web-app-capable" content="yes">
  ${baseStyles()}
  <title>AimTrickHead</title>

  <script>
    (function () {
      var ua = navigator.userAgent || "";
      var isIOS = /iPhone|iPad|iPod/i.test(ua);
      if (!isIOS) {
        document.documentElement.classList.add("non-ios-scroll-fix");
        document.addEventListener("DOMContentLoaded", function () {
          if (document.body) document.body.classList.add("non-ios-scroll-fix");
        });
      }
    })();
  </script>

</head>
<body>
  <div class="bgAura"><div class="orb o1"></div><div class="orb o2"></div><div class="orb o3"></div></div>
  <div class="wrap">
    <div class="homeScreen">
      <div class="homeGlass">
        <div class="heroRow">
          <div class="heroIntro">
            <div class="brand">
              <div class="logoBox">${renderLogo(78, 24)}</div>
              <div>
                <div class="pill">VIP ACTIVE</div>
                <h1 class="heroTitle">AimTrickHead<br>Secure Panel</h1>
                <div class="credit">CRE HUY FANTA - AIMTRICKHEAD UPDATE</div>
              </div>
            </div>
            <p class="heroNote">PANEL.</p>
          </div>
          <div class="pill">huy mkt</div>
        </div>
        <div class="loginPanel">
          <div class="loginIntro">
            <h2 class="loginTitle">Truy cập nhanh</h2>
            <p class="loginHint">kkk.</p>
          </div>
          <div class="grid2" style="margin-top:10px">
            <a class="gameBtn" href="/panel"> Panel</a>
            <a class="socialBtn" href="/free">Nhan key free</a>
          </div>
          <div class="grid2" style="margin-top:12px">
            <a class="socialBtn" href="${ZALO_URL}" target="_blank" rel="noopener noreferrer">${iconZalo()} <span>Zalo</span></a>
            <a class="socialBtn" href="${FACEBOOK_URL}" target="_blank" rel="noopener noreferrer">${iconFacebook()} <span>Facebook</span></a>
          </div>
        </div>
      </div>
    </div>
  </div>
</body>
</html>
  `;
}

function renderAndroidUpdateHtml() {
  return `
<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">
  <meta name="theme-color" content="#05040b">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
  <meta name="mobile-web-app-capable" content="yes">
  <title>AimTrickHead Android Update</title>
  ${baseStyles()}
  <style>
    .androidUpdateWrap{width:min(760px,94vw);margin:0 auto}
    .androidUpdateCard{
      position:relative;overflow:hidden;border-radius:32px;padding:30px 22px;
      background:linear-gradient(180deg, rgba(14,16,24,.92), rgba(9,10,16,.96));
      border:1px solid rgba(255,255,255,.08);
      box-shadow:0 28px 80px rgba(0,0,0,.40), inset 0 1px 0 rgba(255,255,255,.04);
      text-align:center;
    }
    .androidUpdateCard::before{
      content:"";position:absolute;inset:-20% auto auto -10%;width:220px;height:220px;border-radius:50%;
      background:radial-gradient(circle, rgba(168,132,255,.16), transparent 68%);pointer-events:none
    }
    .androidUpdateCard::after{
      content:"";position:absolute;inset:auto -8% -20% auto;width:240px;height:240px;border-radius:50%;
      background:radial-gradient(circle, rgba(228,205,151,.14), transparent 68%);pointer-events:none
    }
    .androidUpdateBadge{
      display:inline-flex;align-items:center;justify-content:center;min-height:30px;padding:0 14px;
      border-radius:999px;background:linear-gradient(180deg, rgba(255,255,255,.07), rgba(255,255,255,.03));
      border:1px solid rgba(255,236,185,.14);color:#e9d6a5;font-size:11px;font-weight:700;letter-spacing:1.1px;text-transform:uppercase;
      box-shadow:inset 0 1px 0 rgba(255,255,255,.04), 0 10px 20px rgba(0,0,0,.12)
    }
    .androidUpdateLogo{
      width:108px;height:108px;margin:18px auto 18px;border-radius:30px;overflow:hidden;background:rgba(255,255,255,.03);
      box-shadow:0 20px 48px rgba(0,0,0,.30), inset 0 1px 0 rgba(255,255,255,.05), 0 0 30px rgba(168,132,255,.14);
      animation:logoFloatGlow 6.8s ease-in-out infinite;
    }
    .androidUpdateTitle{
      margin:0;font-size:clamp(30px,7vw,44px);line-height:1.06;color:#fff7db;
      text-shadow:0 0 20px rgba(255,224,130,.18),0 0 34px rgba(255,255,255,.05)
    }
    .androidUpdateLead{
      margin:14px auto 0;max-width:34rem;color:#efe7ff;font-size:clamp(16px,3.4vw,20px);line-height:1.65;font-weight:700
    }
    .androidUpdateBody{
      margin:18px auto 0;max-width:38rem;color:#d8d4e6;font-size:15px;line-height:1.8;overflow-wrap:anywhere;word-break:break-word
    }
    .androidUpdateBody b{color:#f2dfad}
    .androidUpdateActions{
      display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin-top:22px
    }
      .androidUpdateFooter{
        margin-top:16px;color:#c9c2d8;font-size:13px;line-height:1.7
      }
      @media (max-width:640px){
        .androidUpdateCard{padding:26px 16px}
        .androidUpdateActions{grid-template-columns:1fr}
      }
  </style>
</head>
<body>
  <div class="bgAura"><div class="orb o1"></div><div class="orb o2"></div><div class="orb o3"></div></div>
  <div class="wrap">
    <div class="androidUpdateWrap">
      <div class="androidUpdateCard">
        <div class="androidUpdateBadge">Android Update</div>
        <div class="androidUpdateLogo">${renderLogo(108, 30)}</div>
        <h1 class="androidUpdateTitle">AimTrickHead</h1>
        <div class="androidUpdateLead">Android đã update bản mới.</div>
          <div class="androidUpdateBody">
            Những anh em nào đã mua <b>key vĩnh viễn</b> liên hệ admin app.<br><br>
            Anh em chơi free hóng video sau, Huy update ở <b>video mới nhất</b> hoặc vô <b>bio kênh TikTok</b> lấy bản mới nhất.<br><br>
            Bản mới đã update thêm <b>tâm ảo</b> và vài chức năng hỗ trợ new.<br><br>
            TikTok: <b>Huy FanTa</b>
          </div>
          <div class="androidUpdateActions">
            <a class="socialBtn" href="${TIKTOK_URL}" target="_blank" rel="noopener noreferrer">${iconTikTok(20)} <span>TikTok Huy FanTa</span></a>
            <a class="socialBtn" href="${ZALO_URL}" target="_blank" rel="noopener noreferrer">${iconZalo()} <span>Liên hệ Admin</span></a>
          </div>
        <div class="androidUpdateFooter">Riêng iOS vẫn hoạt động bình thường nha.</div>
      </div>
    </div>
  </div>
</body>
</html>
  `;
}

function renderPanelHtml() {
  return `
<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">
  <meta name="theme-color" content="#05040b">
  <meta name="apple-mobile-web-app-capable" content="yes">
    <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
    <meta name="mobile-web-app-capable" content="yes">
    <title>AimTrickHead</title>
    ${baseStyles()}
    <style>
      .iosSnowLayer{
        position:fixed;inset:0;pointer-events:none;overflow:hidden;z-index:0;opacity:0;
        transition:opacity .35s ease;
      }
      .iosSnowLayer.show{opacity:1}
      .iosSnowFlake{
        position:absolute;top:-12vh;border-radius:50%;
        background:radial-gradient(circle at 35% 35%, rgba(255,255,255,.92), rgba(255,255,255,.18) 70%, transparent 72%);
        filter:drop-shadow(0 0 6px rgba(255,255,255,.10));
        animation-name:iosSnowFall;
        animation-timing-function:linear;
        animation-iteration-count:infinite;
        will-change:transform, opacity;
      }
      @keyframes iosSnowFall{
        0%{transform:translate3d(0,-12vh,0);opacity:0}
        12%{opacity:var(--snowOpacity,.28)}
        100%{transform:translate3d(var(--snowDrift,12px),118vh,0);opacity:0}
      }
    </style>
  
    <script>
      (function () {
        var ua = navigator.userAgent || "";
        var isIOS = /iPhone|iPad|iPod/i.test(ua);
        document.addEventListener("DOMContentLoaded", function () {
          var snowLayer = document.getElementById("iosSnowLayer");
          var cardSnowLayer = document.getElementById("cardSnowLayer");
          if (snowLayer && isIOS) {
            snowLayer.classList.add("show");
          }
          if (cardSnowLayer && isIOS) {
            cardSnowLayer.classList.add("show");
          }
        });
        if (!isIOS) {
          document.documentElement.classList.add("non-ios-scroll-fix");
          document.addEventListener("DOMContentLoaded", function () {
            if (document.body) document.body.classList.add("non-ios-scroll-fix");
          });
      }
    })();
  </script>

  </head>
  <body>
    <div class="bgAura"><div class="orb o1"></div><div class="orb o2"></div><div class="orb o3"></div></div>
    <div class="iosSnowLayer" id="iosSnowLayer" aria-hidden="true">
      <span class="iosSnowFlake" style="left:5%;width:3px;height:3px;animation-duration:24s;animation-delay:-2s;--snowOpacity:.20;--snowDrift:10px"></span>
      <span class="iosSnowFlake" style="left:10%;width:4px;height:4px;animation-duration:28s;animation-delay:-9s;--snowOpacity:.26;--snowDrift:16px"></span>
      <span class="iosSnowFlake" style="left:15%;width:5px;height:5px;animation-duration:26s;animation-delay:-14s;--snowOpacity:.30;--snowDrift:20px"></span>
      <span class="iosSnowFlake" style="left:20%;width:3px;height:3px;animation-duration:23s;animation-delay:-6s;--snowOpacity:.18;--snowDrift:12px"></span>
      <span class="iosSnowFlake" style="left:26%;width:4px;height:4px;animation-duration:27s;animation-delay:-11s;--snowOpacity:.24;--snowDrift:18px"></span>
      <span class="iosSnowFlake" style="left:31%;width:5px;height:5px;animation-duration:25s;animation-delay:-4s;--snowOpacity:.28;--snowDrift:14px"></span>
      <span class="iosSnowFlake" style="left:37%;width:3px;height:3px;animation-duration:29s;animation-delay:-15s;--snowOpacity:.17;--snowDrift:22px"></span>
      <span class="iosSnowFlake" style="left:42%;width:4px;height:4px;animation-duration:24.5s;animation-delay:-8s;--snowOpacity:.23;--snowDrift:13px"></span>
      <span class="iosSnowFlake" style="left:48%;width:5px;height:5px;animation-duration:28.5s;animation-delay:-13s;--snowOpacity:.29;--snowDrift:19px"></span>
      <span class="iosSnowFlake" style="left:53%;width:3px;height:3px;animation-duration:22.5s;animation-delay:-5s;--snowOpacity:.18;--snowDrift:10px"></span>
      <span class="iosSnowFlake" style="left:58%;width:4px;height:4px;animation-duration:27.5s;animation-delay:-16s;--snowOpacity:.24;--snowDrift:17px"></span>
      <span class="iosSnowFlake" style="left:63%;width:5px;height:5px;animation-duration:25.5s;animation-delay:-7s;--snowOpacity:.27;--snowDrift:15px"></span>
      <span class="iosSnowFlake" style="left:69%;width:3px;height:3px;animation-duration:30s;animation-delay:-12s;--snowOpacity:.16;--snowDrift:24px"></span>
      <span class="iosSnowFlake" style="left:74%;width:4px;height:4px;animation-duration:23.5s;animation-delay:-3s;--snowOpacity:.22;--snowDrift:11px"></span>
      <span class="iosSnowFlake" style="left:80%;width:5px;height:5px;animation-duration:28s;animation-delay:-10s;--snowOpacity:.28;--snowDrift:18px"></span>
      <span class="iosSnowFlake" style="left:85%;width:3px;height:3px;animation-duration:26.5s;animation-delay:-17s;--snowOpacity:.18;--snowDrift:14px"></span>
      <span class="iosSnowFlake" style="left:90%;width:4px;height:4px;animation-duration:24s;animation-delay:-6s;--snowOpacity:.23;--snowDrift:12px"></span>
      <span class="iosSnowFlake" style="left:95%;width:5px;height:5px;animation-duration:29s;animation-delay:-14s;--snowOpacity:.26;--snowDrift:20px"></span>
        </div>
    <div class="statusBar">
      <div class="statusTime" id="statusTime"></div>
      <div class="statusMeta">
        <span class="statusDot"></span>
        <span class="statusLabel">PREMIUM AIM TRICK HEAD</span>
      
    </div>
  </div>

  <div class="loadingLayer" id="loadingLayer">
    <div class="loadingLogo">${renderLogo(172, 28)}</div>
    <div class="loadingText">AimTrickHead </div>
    <div class="loadingSub">Đang Tải Chờ Xíu...</div>
    <div class="loadingBar"><span></span></div>
  </div>

    <div class="wrap">
      <div class="card">
        <div class="cardSnowLayer" id="cardSnowLayer" aria-hidden="true">
          <span class="cardSnowFlake" style="left:4%;width:3px;height:3px;animation-duration:28s;animation-delay:-2s;--snowOpacity:.20;--snowDrift:10px"></span>
          <span class="cardSnowFlake" style="left:8%;width:4px;height:4px;animation-duration:31s;animation-delay:-12s;--snowOpacity:.24;--snowDrift:16px"></span>
          <span class="cardSnowFlake" style="left:12%;width:5px;height:5px;animation-duration:29s;animation-delay:-6s;--snowOpacity:.28;--snowDrift:14px"></span>
          <span class="cardSnowFlake" style="left:16%;width:6px;height:6px;animation-duration:33s;animation-delay:-18s;--snowOpacity:.34;--snowDrift:20px"></span>
          <span class="cardSnowFlake" style="left:20%;width:3px;height:3px;animation-duration:27s;animation-delay:-9s;--snowOpacity:.18;--snowDrift:12px"></span>
          <span class="cardSnowFlake" style="left:24%;width:4px;height:4px;animation-duration:30s;animation-delay:-14s;--snowOpacity:.23;--snowDrift:18px"></span>
          <span class="cardSnowFlake" style="left:28%;width:5px;height:5px;animation-duration:32s;animation-delay:-5s;--snowOpacity:.29;--snowDrift:22px"></span>
          <span class="cardSnowFlake" style="left:33%;width:3px;height:3px;animation-duration:26s;animation-delay:-16s;--snowOpacity:.17;--snowDrift:9px"></span>
          <span class="cardSnowFlake" style="left:37%;width:4px;height:4px;animation-duration:29.5s;animation-delay:-8s;--snowOpacity:.24;--snowDrift:15px"></span>
          <span class="cardSnowFlake" style="left:42%;width:5px;height:5px;animation-duration:31.5s;animation-delay:-20s;--snowOpacity:.30;--snowDrift:19px"></span>
          <span class="cardSnowFlake" style="left:47%;width:6px;height:6px;animation-duration:34s;animation-delay:-11s;--snowOpacity:.36;--snowDrift:24px"></span>
          <span class="cardSnowFlake" style="left:52%;width:3px;height:3px;animation-duration:27.5s;animation-delay:-4s;--snowOpacity:.18;--snowDrift:10px"></span>
          <span class="cardSnowFlake" style="left:57%;width:4px;height:4px;animation-duration:30.5s;animation-delay:-17s;--snowOpacity:.24;--snowDrift:16px"></span>
          <span class="cardSnowFlake" style="left:62%;width:5px;height:5px;animation-duration:32.5s;animation-delay:-7s;--snowOpacity:.30;--snowDrift:20px"></span>
          <span class="cardSnowFlake" style="left:67%;width:6px;height:6px;animation-duration:35s;animation-delay:-13s;--snowOpacity:.34;--snowDrift:18px"></span>
          <span class="cardSnowFlake" style="left:72%;width:3px;height:3px;animation-duration:28.5s;animation-delay:-21s;--snowOpacity:.17;--snowDrift:11px"></span>
          <span class="cardSnowFlake" style="left:77%;width:4px;height:4px;animation-duration:31s;animation-delay:-10s;--snowOpacity:.23;--snowDrift:14px"></span>
          <span class="cardSnowFlake" style="left:82%;width:5px;height:5px;animation-duration:33s;animation-delay:-3s;--snowOpacity:.28;--snowDrift:21px"></span>
          <span class="cardSnowFlake" style="left:87%;width:6px;height:6px;animation-duration:34.5s;animation-delay:-15s;--snowOpacity:.35;--snowDrift:17px"></span>
          <span class="cardSnowFlake" style="left:92%;width:4px;height:4px;animation-duration:29s;animation-delay:-19s;--snowOpacity:.22;--snowDrift:12px"></span>
          <span class="cardSnowFlake" style="left:96%;width:5px;height:5px;animation-duration:32s;animation-delay:-6s;--snowOpacity:.27;--snowDrift:19px"></span>
        </div>
        <div class="top">
          <div class="brand">
            <div class="logoBox">${renderLogo(88, 26)}</div>
            <div>
            <h1 class="title">AimTrickHead</h1>
            <div class="sub">Key Active By Huy Fanta</div>
            <div class="credit">CRE HUY FANTA - AimTrickHead</div>
          </div>
        </div>
      </div>

      <div class="content">
        <div class="luxBar"><div class="luxLeft"><span class="luxDot"></span><span>Update Aim Trick Head Mobile</span></div><div class="luxRight"></div></div>
        <div class="loginFeatureList"><div class="loginFeatureItem"><span class="loginFeatureTick">✓</span><span>Hoạt Động Ổn Định Mượt Mà Trên Adr Và iOS</span></div><div class="loginFeatureItem"><span class="loginFeatureTick">✓</span><span>An Toàn Tài Khoản Game</span></div><div class="loginFeatureItem"><span class="loginFeatureTick">✓</span><span>Tự Động Update</span></div></div>
        <div id="loginView" class="loginHero" style="background:transparent;border:none;border-radius:0;box-shadow:none;min-height:auto;display:flex;align-items:stretch;justify-content:flex-start">
          <div class="loginFrame">
          <div class="loginIntro">
            <div class="loginEyebrow">Premium Login</div>
            <h2 class="loginTitle">AimTrickHead</h2>
            <p class="loginHint">
             APP Hỗ Trợ Kéo Tâm Trên ADR - IOS AimTrickHead.
            </p>
            <div class="loginAccent"></div>
          </div>
          
          <input id="keyInput" class="input" type="text" placeholder="Nhập Key Vào Đây Đi Nè" autocapitalize="off" autocomplete="off" spellcheck="false" enterkeyhint="done">
          <button class="btn" onclick="dangNhap()">Đăng nhập</button>
          <div class="loginIconRow">
            <a class="loginIconLink" href="${ZALO_URL}" target="_blank" rel="noopener noreferrer" aria-label="Zalo">${iconZalo(56)}</a>
            <a class="loginIconLink" href="${FACEBOOK_URL}" target="_blank" rel="noopener noreferrer" aria-label="Facebook">${iconFacebook(56)}</a>
          </div>
          <a class="socialBtn" href="/free" style="margin-top:16px;width:min(280px,100%);align-self:center">Lấy Key Free !</a>
          <div id="msg" class="msg"></div>
          </div>
        </div>

        <div id="panelView" class="hidden">
          <div class="topLine">
            <div class="pill">✦ VIP ACTIVE</div>
            <button class="smallBtn" onclick="dangXuat()">Thoát</button>
          </div>

          <div class="quickStats">
            <div class="statChip"><span class="statLabel">Support</span><strong>Hỗ Trợ FF</strong></div>
            <div class="statChip"><span class="statLabel">Support</span><strong>Hỗ Trợ FFMAX</strong></div>
            <div class="statChip"><span class="statLabel">Device</span><strong>All Mobile</strong></div>
          </div>

          <div class="noticeBox" id="keyNotice">
            Key đang hoạt động.
          </div>

            <div class="tabs">
              <button class="tab active" data-tab="tab1">Main</button>
              <button class="tab" data-tab="tab2">Optimize+</button>
              <button class="tab" data-tab="tab4">Social</button>
              <button class="tab" data-tab="tab6">TikTok</button>
            </div>

          <div id="tab1" class="tabPane active">
            <div class="tile"><div class="row"><div><p class="name">AimTrickHead</p><p class="desc">Chức Năng Hỗ Trợ Kéo Tâm Trong APP</p></div><label class="switch"><input type="checkbox" id="f2" onchange="toggleFx(this,'AimTrickHead')"><span class="slider"></span></label></div></div>
            <div class="tile"><div class="row"><div><p class="name">Bám Đầu</p><p class="desc">Tác dụng hỗ trợ bám</p></div><label class="switch"><input type="checkbox" id="f3" onchange="toggleFx(this,'Bám Đầu')"><span class="slider"></span></label></div></div>
            <div class="tile"><div class="row"><div><p class="name">Nhẹ Tâm</p><p class="desc">Tác dụng nhẹ tâm</p></div><label class="switch"><input type="checkbox" id="f4" onchange="toggleFx(this,'Nhẹ Tâm')"><span class="slider"></span></label></div></div>
          </div>

          <div id="tab2" class="tabPane">
            <div class="tile"><div class="row"><div><p class="name">Tối Ưu Mạnh</p><p class="desc">Tăng phản hồi ngay sau khi bật nhen</p></div><label class="switch"><input type="checkbox" id="f5" onchange="toggleFx(this,'Tối Ưu Mạnh')"><span class="slider"></span></label></div></div>
            <div class="tile"><div class="row"><div><p class="name">Buff Nhạy x Nhẹ Tâm</p><p class="desc">Tối ưu cảm giác kéo và độ mượt</p></div><label class="switch"><input type="checkbox" id="f6" onchange="toggleFx(this,'Buff Nhạy x Nhẹ Tâm')"><span class="slider"></span></label></div></div>
            <div class="tile"><div class="row"><div><p class="name">Game Boost</p><p class="desc">Tối Ưu Mượt Game</p></div><label class="switch"><input type="checkbox" id="f7" onchange="toggleFx(this,'Game Boost')"><span class="slider"></span></label></div></div>
            <div class="tile"><div class="row"><div><p class="name">Nhẹ Tâm + Fix Rung</p><p class="desc">Giảm Rung Nhẹ</p></div><label class="switch"><input type="checkbox" id="f1" onchange="toggleFx(this,'Nhẹ Tâm + Fix Rung')"><span class="slider"></span></label></div></div>
            <div class="tile">
              <p class="name">Sensi Control</p>
              <p class="desc">Sensi Mobile</p>
              <div class="sliderWrap">
                <div class="rangeLabel"><span>Level</span><span id="sensiValue">60</span></div>
                <input type="range" min="1" max="120" value="60" id="sensiRange" oninput="updateSensi(this.value)">
              </div>
            </div>
          </div>

            <div id="tab4" class="tabPane">
              <div class="grid2">
                <a class="socialBtn" href="${ZALO_URL}" target="_blank" rel="noopener noreferrer">${iconZalo()} <span>Liên hệ Zalo</span></a>
                <a class="socialBtn" href="${FACEBOOK_URL}" target="_blank" rel="noopener noreferrer">${iconFacebook()} <span>Facebook</span></a>
            </div>
            <div class="footer">Mua Key Vĩnh Viễn Hoặc Hỗ Trợ Liên Hệ.</div>
          </div>

          <div id="tab6" class="tabPane">
            <div class="grid2">
              <a class="socialBtn" href="${TIKTOK_URL}" target="_blank" rel="noopener noreferrer">${iconTikTok(20)} <span>TikTok</span></a>
              <a class="socialBtn" href="${ZALO_URL}" target="_blank" rel="noopener noreferrer">${iconZalo()} <span>Liên hệ Admin</span></a>
            </div>
            <div class="footer">
              Theo Dõi Thông Tin Về APP , Files Aimlock , Kéo Tâm.<br>
              Key Vĩnh Viễn Liên Hệ.
            </div>
          </div>

            <div class="liveFx" id="liveFxBox"><span class="fxLine">⚡ Chờ kích hoạt module...</span></div>
        </div>
      </div>
    </div>
  </div>


  <div id="fxOverlay" class="fxOverlay hidden">
    <div class="fxGlow"></div>
    <div class="fxOverlayCard">
      <div class="fxOverlayTitle" id="fxOverlayTitle">ĐÃ BẬT</div>
      <div class="fxOverlaySub" id="fxOverlaySub">Chức năng đang hoạt động</div>
    </div>
  </div>

  <div id="gamePrepOverlay" class="fxOverlay hidden">
    <div class="fxGlow"></div>
    <div class="fxOverlayCard prepCard">
      <div class="prepSpinner"></div>
      <div class="fxOverlayTitle" id="gamePrepTitle">ĐANG SETUP</div>
      <div class="fxOverlaySub" id="gamePrepSub">Đang cấu hình tính năng...</div>
      <div class="prepBar"><span></span></div>
    </div>
  </div>

  <div id="gameDoneOverlay" class="fxOverlay hidden">
    <div class="fxGlow"></div>
    <div class="fxOverlayCard">
      <div class="fxOverlayTitle">ĐÃ SETUP XONG</div>
      <div class="fxOverlaySub" id="gameDoneSub">Thoát ra và vào game là được nhé bro</div>
    </div>
  </div>

  <div id="toast" class="toast"></div>

  <script>
    const msg = document.getElementById("msg");
    const loginView = document.getElementById("loginView");
    const panelView = document.getElementById("panelView");
    const toast = document.getElementById("toast");
    const liveFxBox = document.getElementById("liveFxBox");
    const fxOverlay = document.getElementById("fxOverlay");
    const fxOverlayTitle = document.getElementById("fxOverlayTitle");
    const fxOverlaySub = document.getElementById("fxOverlaySub");
    const gamePrepOverlay = document.getElementById("gamePrepOverlay");
    const gamePrepTitle = document.getElementById("gamePrepTitle");
    const gamePrepSub = document.getElementById("gamePrepSub");
    const gameDoneOverlay = document.getElementById("gameDoneOverlay");
    const gameDoneSub = document.getElementById("gameDoneSub");
    const sensiValue = document.getElementById("sensiValue");
    const loadingLayer = document.getElementById("loadingLayer");
    const keyNotice = document.getElementById("keyNotice");
    const statusTime = document.getElementById("statusTime");
    const statusBatteryRemoved = document.getElementById("statusBatteryRemoved");
    const keyInputEl = document.getElementById("keyInput");

    const FF_ANDROID_PACKAGE = ${JSON.stringify(FF_ANDROID_PACKAGE)};
    const FFMAX_ANDROID_PACKAGE = ${JSON.stringify(FFMAX_ANDROID_PACKAGE)};
    const FF_IOS_SCHEME = ${JSON.stringify(FF_IOS_SCHEME)};
    const FFMAX_IOS_SCHEME = ${JSON.stringify(FFMAX_IOS_SCHEME)};
    const FF_URL = ${JSON.stringify(FF_URL)};
    const FF_MAX_URL = ${JSON.stringify(FF_MAX_URL)};
    const FF_IOS_STORE = ${JSON.stringify("https://apps.apple.com/app/id" + FF_IOS_APPID)};
    const FFMAX_IOS_STORE = ${JSON.stringify("https://apps.apple.com/app/id" + FFMAX_IOS_APPID)};

    let fxTimer = null;
    let codeTimer = null;
    let codeRunToken = 0;


    function updateStatusBar() {
      const nowText = new Intl.DateTimeFormat("vi-VN", {
        timeZone: "Asia/Ho_Chi_Minh",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false
      }).format(new Date());
      statusTime.textContent = nowText;
    }

    async function updateBattery() {
      try {
        if (!navigator.getBattery) return;
        const battery = await navigator.getBattery();
        const paint = function() {
          statusBatteryRemoved.textContent = Math.round(battery.level * 100) + "%";
        };
        paint();
        battery.addEventListener("levelchange", paint);
      } catch (e) {}
    }

    function lockAppShellScroll() {
      let startY = 0;
      document.addEventListener("touchstart", function (e) {
        startY = e.touches && e.touches[0] ? e.touches[0].clientY : 0;
      }, { passive: true });

      document.addEventListener("touchmove", function (e) {
        const target = e.target && e.target.closest ? e.target.closest(".card,.homeGlass") : null;
        if (!target) {
          e.preventDefault();
          return;
        }
        const currentY = e.touches && e.touches[0] ? e.touches[0].clientY : startY;
        const delta = currentY - startY;
        const atTop = target.scrollTop <= 0;
        const atBottom = target.scrollTop + target.clientHeight >= target.scrollHeight - 1;
        if ((atTop && delta > 0) || (atBottom && delta < 0)) {
          e.preventDefault();
        }
      }, { passive: false });
    }

    function hideLoading() {
      setTimeout(function () {
        loadingLayer.classList.add("hide");
      }, 1800);
    }

    function showToast(text, type) {
      toast.className = "toast show " + (type || "");
      toast.textContent = text || "";
      setTimeout(function () { toast.className = "toast"; }, 2200);
    }

    function getDevice() {
      let id = localStorage.getItem("ath_device");
      if (!id) {
        id = "web-" + Math.random().toString(36).slice(2, 12);
        localStorage.setItem("ath_device", id);
      }
      return id;
    }

    function setMsg(text, type) {
      msg.textContent = text || "";
      msg.className = "msg " + (type || "");
    }

    function saveSession(data) {
      localStorage.setItem("ath_session", data.token || "");
      localStorage.setItem("ath_key", data.key || "");
    }

    function getSession() { return localStorage.getItem("ath_session"); }
    function getSavedKey() { return localStorage.getItem("ath_key") || ""; }
    function getStateStorageKey(key) {
      const device = getDevice();
      const activeKey = String(key || getSavedKey() || "").trim();
      return "ath_state_" + device + "_" + activeKey;
    }
    function getSensiStorageKey(key) {
      const device = getDevice();
      const activeKey = String(key || getSavedKey() || "").trim();
      return "ath_sensi_" + device + "_" + activeKey;
    }
    function clearSession() {
      localStorage.removeItem("ath_session");
      localStorage.removeItem("ath_key");
    }

    function msToViDuration(ms) {
      if (!ms) return "Vĩnh viễn";
      if (ms <= 0) return "0 phút";
      const totalMinutes = Math.floor(ms / 60000);
      const days = Math.floor(totalMinutes / (60 * 24));
      const hours = Math.floor((totalMinutes % (60 * 24)) / 60);
      const minutes = totalMinutes % 60;
      const parts = [];
      if (days) parts.push(days + " ngày");
      if (hours) parts.push(hours + " giờ");
      if (minutes || parts.length === 0) parts.push(minutes + " phút");
      return parts.slice(0, 3).join(" ");
    }

    function buildNotice(data) {
      const keyText = data.key || getSavedKey() || "Đang hoạt động";
      const remainText = !data.expireAt ? "Vĩnh viễn" : msToViDuration((data.expireAt || 0) - Date.now());
      keyNotice.innerHTML =
        '<b>Key:</b> ' + keyText +
        '<br><b>Hiệu lực còn:</b> ' + remainText +
        '<br><b>Hết hạn lúc:</b> ' + (data.expireText || "--");
    }

    function escapeHtml(text) {
      return String(text)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function writeFx(lines) {
        const hud =
          '<div class="liveFxHud"><div class="hudDots"><span></span><span></span><span></span></div><div class="fxMeta"><span>runtime monitor</span><span class="fxMeter"></span><span>locked</span></div></div>' +
          '<div class="liveFxControls"><button class="monitorGameBtn" onclick="prepGame(\'FF\')">FF</button><button class="monitorGameBtn" onclick="prepGame(\'FFMAX\')">FFMAX</button></div>';
        const body = lines.map(function(line){
          if (typeof line === "string") {
            return '<span class="fxLine">' + escapeHtml(line) + '</span>';
          }
        const tone = line && line.tone ? " " + line.tone : "";
        const value = line && line.text ? line.text : "";
        return '<span class="fxLine' + tone + '">' + escapeHtml(value) + '</span>';
      }).join("");
      liveFxBox.innerHTML = hud + body + '<span class="fxCursor"></span>';
    }

    function makeStamp() {
      const now = new Date();
      const h = String(now.getHours()).padStart(2, "0");
      const m = String(now.getMinutes()).padStart(2, "0");
      const s = String(now.getSeconds()).padStart(2, "0");
      return h + ":" + m + ":" + s;
    }

    function randomHex(size) {
      return Math.random().toString(16).slice(2, 2 + size).toUpperCase().padEnd(size, "0");
    }

    function runFxSequence(lines, interval, onDone) {
      const token = ++codeRunToken;
      let index = 0;
      function step() {
        if (token !== codeRunToken) return;
        writeFx(lines.slice(0, index + 1));
        index += 1;
        if (index < lines.length) {
          codeTimer = setTimeout(step, interval);
        } else if (typeof onDone === "function") {
          codeTimer = setTimeout(function () {
            if (token === codeRunToken) onDone();
          }, 900);
        }
      }
      step();
    }

    function startFxFeed() {
      clearInterval(fxTimer);
      liveFxBox.classList.remove("activePulse");
      const lines = [
        [{ text: "[" + makeStamp() + "] runtime.idle => premium-ready", tone: "info" }, { text: "-> state cache verified", tone: "ok" }, { text: "-> running app", tone: "ok" }, { text: "-> freefire smooth", tone: "ok" }, { text: "-> shell depth = 4.0", tone: "info" }],
        [{ text: "[" + makeStamp() + "] ui.pipeline => stable", tone: "info" }, { text: "-> stream active", tone: "ok" }, { text: "-> auto update", tone: "ok" }, { text: "-> com.dts.ffth", tone: "info" }, { text: "-> ok", tone: "ok" }],
        [{ text: "[" + makeStamp() + "] mobile.profile => online", tone: "info" }, { text: "-> update all", tone: "ok" }, { text: "-> sensitivity on", tone: "ok" }, { text: "-> com.dts.ffmax", tone: "ok" }, { text: "-> overscroll bounce = 0", tone: "ok" }],
        [{ text: "[" + makeStamp() + "] optimize.stream => armed", tone: "info" }, { text: "-> online", tone: "ok" }, { text: "-> module ready", tone: "ok" }, { text: "-> cache drift = 0.00", tone: "info" }, { text: "-> freefire lock fps", tone: "ok" }],
        [{ text: "[" + makeStamp() + "] premium.tabs => elite", tone: "info" }, { text: "-> on files", tone: "ok" }, { text: "-> smooth render", tone: "ok" }, { text: "-> shell locked", tone: "warn" }, { text: "-> lock fps", tone: "ok" }]
      ];
      let i = 0;
      writeFx(lines[0]);
      fxTimer = setInterval(function () {
        writeFx(lines[i % lines.length]);
        i++;
      }, 1900);
    }

    function simulateCodeRun(label, isOn) {
      clearInterval(fxTimer);
      clearTimeout(codeTimer);
      liveFxBox.classList.add("activePulse");
      const moduleName = label.replaceAll(" ", "_").toLowerCase();
      const seq = [
        { text: "[" + makeStamp() + "] > wake.runtime --shell=ath-v4", tone: "info" },
        { text: "[" + makeStamp() + "] > select.module --name=" + moduleName, tone: "info" },
        { text: "[" + makeStamp() + "] > auth.runtime --token=" + randomHex(8), tone: "info" },
        { text: "[" + makeStamp() + "] > mount.profile --tier=vip-signature", tone: "ok" },
        { text: "[" + makeStamp() + "] > patch.touch --vector=" + randomHex(6), tone: "ok" },
        { text: "[" + makeStamp() + "] > patch.motion --curve=" + randomHex(5), tone: "ok" },
        { text: "[" + makeStamp() + "] > patch.visual --layer=glass-core-s", tone: "ok" },
        { text: "[" + makeStamp() + "] > patch.viewport --bounce=off --mode=fixed", tone: "ok" },
        { text: "[" + makeStamp() + "] > patch.render --alpha=" + (isOn ? "0.96" : "0.22"), tone: "info" },
        { text: "[" + makeStamp() + "] > sync.kernel --pulse=" + randomHex(4), tone: "info" },
        { text: "[" + makeStamp() + "] > lock.viewport --mode=app-shell", tone: "ok" },
        { text: "[" + makeStamp() + "] > attach_runtime --mode=" + (isOn ? "enable" : "disable"), tone: "warn" },
        { text: "[" + makeStamp() + "] > verify.commit --hash=" + randomHex(10), tone: "info" },
        { text: isOn ? "[" + makeStamp() + "] > status => ACTIVE - LOCKED - STABLE" : "[" + makeStamp() + "] > status => OFF - RELEASED - SAFE", tone: isOn ? "ok" : "warn" }
      ];
      runFxSequence(seq, 92, function () {
        liveFxBox.classList.remove("activePulse");
        startFxFeed();
      });
    }

    function tryOpenUrl(url) {
      try {
        const iframe = document.createElement("iframe");
        iframe.style.display = "none";
        iframe.src = url;
        document.body.appendChild(iframe);
        setTimeout(function () {
          iframe.remove();
        }, 1200);
      } catch (e) {}
      try {
        window.location.href = url;
      } catch (e) {}
    }

    function launchGame(target) {
      const isMax = target === "FFMAX";
      const fallback = isMax ? FF_MAX_URL : FF_URL;
      const scheme = isMax ? FFMAX_IOS_SCHEME : FF_IOS_SCHEME;

      function openFallback() {
        window.location.href = fallback;
      }

      if (isAndroid()) {
        openFallback();
        return;
      }

      if (isIOS()) {
        tryOpenUrl(scheme);
        return;
      }

      openFallback();
    }

    function moPanel(data) {
      loginView.classList.add("hidden");
      panelView.classList.remove("hidden");
      panelView.classList.remove("panelReveal");
      void panelView.offsetWidth;
      panelView.classList.add("panelReveal");
      buildNotice(data);
      taiTrangThai(data && data.key ? data.key : getSavedKey());
      startFxFeed();
    }

    function dangXuat() {
      clearSession();
      clearInterval(fxTimer);
      clearTimeout(codeTimer);
      panelView.classList.add("hidden");
      loginView.classList.remove("hidden");
      if (keyInputEl) keyInputEl.value = "";
      setMsg("", "");
      showToast("Đã thoát", "err");
    }

    async function dangNhap() {
      const key = keyInputEl ? keyInputEl.value.trim() : "";
      if (!key) {
        setMsg("Vui lòng nhập key.", "err");
        return;
      }
      setMsg("Đang kiểm tra key...");
      try {
        const res = await fetch("/api/check", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ key: key, device: getDevice() })
        });
        const data = await res.json();
        if (!data.ok) {
          setMsg(data.msg || "Đăng nhập thất bại.", "err");
          return;
        }
        saveSession({ token: data.token, key: key });
        data.key = key;
        setMsg("Đăng nhập thành công.", "ok");
        showToast("Đăng nhập thành công", "ok");
        moPanel(data);
      } catch (e) {
        setMsg("Không thể kết nối tới máy chủ.", "err");
      }
    }

    function showBigFx(label, isOn) {
      fxOverlayTitle.textContent = isOn ? "ĐÃ BẬT" : "ĐÃ TẮT";
      fxOverlaySub.textContent = label + (isOn ? " đang hoạt động" : " đã ngừng hoạt động");
      fxOverlay.classList.remove("hidden");
      clearTimeout(window.__fxOverlayTimer);
      window.__fxOverlayTimer = setTimeout(function () {
        fxOverlay.classList.add("hidden");
      }, 1600);
    }

    function toggleFx(el, label) {
      luuTrangThai();
      simulateCodeRun(label, el.checked);
      showToast(label + (el.checked ? " đã bật" : " đã tắt"), el.checked ? "ok" : "err");
      showBigFx(label, el.checked);
    }

    function updateSensi(val) {
      sensiValue.textContent = val;
      localStorage.setItem(getSensiStorageKey(), String(val));
      simulateCodeRun("Sensi_" + val, true);
    }

    function getDefaultState() {
      return { f1:false, f2:false, f3:false, f4:false, f5:false, f6:false, f7:false };
    }

    function luuTrangThai() {
      const state = {
        f1: document.getElementById("f1") ? document.getElementById("f1").checked : false,
        f2: document.getElementById("f2") ? document.getElementById("f2").checked : false,
        f3: document.getElementById("f3") ? document.getElementById("f3").checked : false,
        f4: document.getElementById("f4") ? document.getElementById("f4").checked : false,
        f5: document.getElementById("f5") ? document.getElementById("f5").checked : false,
        f6: document.getElementById("f6") ? document.getElementById("f6").checked : false,
        f7: document.getElementById("f7") ? document.getElementById("f7").checked : false
      };
      localStorage.setItem(getStateStorageKey(), JSON.stringify(state));
    }

    function taiTrangThai(key) {
      try {
        const state = Object.assign(getDefaultState(), JSON.parse(localStorage.getItem(getStateStorageKey(key)) || "{}"));
        ["f1","f2","f3","f4","f5","f6","f7"].forEach(function (id) {
          const el = document.getElementById(id);
          if (el) el.checked = !!state[id];
        });
        const savedSensi = localStorage.getItem(getSensiStorageKey(key)) || "60";
        const sensiRange = document.getElementById("sensiRange");
        if (sensiRange) sensiRange.value = savedSensi;
        sensiValue.textContent = savedSensi;
      } catch (e) {
        const defaults = getDefaultState();
        ["f1","f2","f3","f4","f5","f6","f7"].forEach(function (id) {
          const el = document.getElementById(id);
          if (el) el.checked = !!defaults[id];
        });
        sensiValue.textContent = "60";
      }
    }

    function isAndroid() {
      return /Android/i.test(navigator.userAgent || "");
    }

    function isIOS() {
      return /iPhone|iPad|iPod/i.test(navigator.userAgent || "");
    }

    function prepGame(target) {
      const pretty = target === "FFMAX" ? "FF MAX" : "FREE FIRE";
      gamePrepTitle.textContent = "ĐANG SETUP " + pretty;
      gamePrepSub.textContent = "Đang đồng bộ preset, module và trạng thái đã bật";
      gamePrepOverlay.classList.remove("hidden");
      gameDoneOverlay.classList.add("hidden");
      simulateCodeRun("launch_" + pretty, true);
      showToast("Đang setup " + pretty, "ok");

      if (isAndroid()) {
        setTimeout(function () {
          gamePrepOverlay.classList.add("hidden");
          gameDoneSub.textContent = "Đã hoàn thành bật các chỉnh sửa data, vui lòng thoát app mở game thủ công";
          gameDoneOverlay.classList.remove("hidden");
          showToast("Đã hoàn thành bật các chỉnh sửa data, vui lòng thoát app mở game thủ công", "ok");
          setTimeout(function () {
            gameDoneOverlay.classList.add("hidden");
          }, 2800);
        }, 5000);
        return;
      }

      setTimeout(function () {
        gamePrepOverlay.classList.add("hidden");
        gameDoneSub.textContent = pretty + " đã setup xong. Đang mở game, nếu lỗi thì thoát app vô game nhé.";
        gameDoneOverlay.classList.remove("hidden");
        showToast(pretty + " đã setup xong", "ok");
        setTimeout(function () {
          launchGame(target);
        }, 550);
        setTimeout(function () {
          gameDoneOverlay.classList.add("hidden");
        }, 2300);
      }, 2600);
    }

    document.querySelectorAll(".tab").forEach(function (btn) {
      btn.addEventListener("click", function () {
        document.querySelectorAll(".tab").forEach(function (b) { b.classList.remove("active"); });
        document.querySelectorAll(".tabPane").forEach(function (p) { p.classList.remove("active"); });
        btn.classList.add("active");
        const pane = document.getElementById(btn.dataset.tab);
        if (pane) pane.classList.add("active");
      });
    });

    window.addEventListener("touchmove", function () {}, { passive: true });

    window.addEventListener("load", async function () {
      hideLoading();
      const token = getSession();
      if (!token) return;
      try {
        const res = await fetch("/api/status", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ token: token, device: getDevice() })
        });
        const data = await res.json();
        if (data.ok) {
          data.key = getSavedKey();
          moPanel(data);
        } else {
          clearSession();
        }
      } catch (e) {}
    });
  </script>
</body>
</html>
  `;
}

function renderAdminHtml() {
  return `
<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">
  <meta name="theme-color" content="#05040b">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
  <meta name="mobile-web-app-capable" content="yes">
  <title>Admin Dashboard</title>
  ${baseStyles()}
  <style>
    .adminWrap {
      width: min(1080px, 100%);
      margin: 0 auto;
    }

    .adminStack {
      display: grid;
      gap: 16px;
    }
    .adminHidden {
      display: none !important;
    }
    .adminHero {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
      margin-bottom: 8px;
    }
    .adminEyebrow {
      font-size: 12px;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      color: #d6c28f;
      margin-bottom: 8px;
    }
    .adminHeading {
      margin: 0;
      font-size: clamp(28px, 4vw, 42px);
      line-height: 1.05;
    }
    .adminSub {
      margin-top: 10px;
      color: var(--muted);
      font-size: 15px;
      line-height: 1.7;
      max-width: 720px;
    }
    .panelBlock {
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 24px;
      padding: 20px;
      background:
        linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0.015)),
        rgba(8, 8, 14, 0.84);
      box-shadow: 0 18px 46px rgba(0,0,0,0.28);
    }
    .panelTitle {
      margin: 0 0 14px;
      font-size: 21px;
      font-weight: 800;
    }
    .panelHint {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.7;
      margin-bottom: 14px;
    }
    .adminGrid {
      display: grid;
      gap: 16px;
      grid-template-columns: 1.1fr 0.9fr;
    }
    .statsGrid {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }
    .statCard {
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 22px;
      padding: 18px;
      background: rgba(255,255,255,0.025);
      min-height: 126px;
    }
    .statLabel {
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 10px;
      line-height: 1.5;
    }
    .statValue {
      font-size: clamp(26px, 3.2vw, 38px);
      font-weight: 900;
      line-height: 1;
      margin-bottom: 8px;
    }
    .statMeta {
      font-size: 13px;
      color: #f3dfaa;
      line-height: 1.5;
    }
    .toolbarLine {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 14px;
    }
    .toolbarBadge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid rgba(255,255,255,0.08);
      background: rgba(255,255,255,0.03);
      font-size: 13px;
      color: var(--muted);
    }
    .adminActions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    .miniBtn {
      height: 48px;
      padding: 0 18px;
      border-radius: 16px;
      border: 1px solid rgba(255,255,255,0.12);
      background: rgba(255,255,255,0.04);
      color: var(--text);
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
    }
    .miniBtn:hover {
      background: rgba(255,255,255,0.07);
    }
    .searchRow,
    .formRow {
      display: grid;
      gap: 10px;
      grid-template-columns: 1fr auto;
    }
    .listWrap {
      display: grid;
      gap: 12px;
    }
    .keyRow {
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 20px;
      padding: 16px;
      background: rgba(255,255,255,0.022);
    }
    .keyRowTop {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }
    .keyCode {
      font-size: 16px;
      font-weight: 800;
      word-break: break-all;
      margin-bottom: 10px;
    }
    .keyMeta {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.6;
    }
    .keySource {
      display: inline-flex;
      align-items: center;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(214,194,143,0.14);
      border: 1px solid rgba(214,194,143,0.2);
      color: #f3dfaa;
      font-size: 12px;
      font-weight: 800;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .loginCard {
      max-width: 580px;
      margin: 0 auto;
    }
    .debugBox {
      margin-top: 12px;
      padding: 12px 14px;
      border-radius: 16px;
      border: 1px solid rgba(255,255,255,0.08);
      background: rgba(255,255,255,0.03);
      color: var(--muted);
      font-size: 13px;
      line-height: 1.65;
      word-break: break-word;
    }
    .debugLine {
      margin-top: 6px;
    }
    .debugLine:first-child {
      margin-top: 0;
    }
    @media (max-width: 900px) {
      .adminGrid,
      .statsGrid {
        grid-template-columns: 1fr;
      }
    }
    @media (max-width: 720px) {
      .panelBlock {
        padding: 18px;
        border-radius: 22px;
      }
      .searchRow,
      .formRow {
        grid-template-columns: 1fr;
      }
      .miniBtn,
      .btn,
      .smallBtn {
        width: 100%;
      }
    }
  </style>

  <script>
    (function () {
      var ua = navigator.userAgent || "";
      var isIOS = /iPhone|iPad|iPod/i.test(ua);
      if (!isIOS) {
        document.documentElement.classList.add("non-ios-scroll-fix");
        document.addEventListener("DOMContentLoaded", function () {
          if (document.body) document.body.classList.add("non-ios-scroll-fix");
        });
      }
    })();
  </script>

</head>
<body>
  <div class="bgAura"><div class="orb o1"></div><div class="orb o2"></div><div class="orb o3"></div></div>
  <div class="wrap">
    <div class="card adminWrap">
      <div class="top">
        <div class="brand" style="width:100%;justify-content:space-between;gap:18px;flex-wrap:wrap">
          <div class="logoBox">${renderLogo(74,22)}</div>
          <div>
            <div class="adminEyebrow">Admin Route</div>
            <h1 class="adminHeading">Dashboard quản lý key</h1>
            <div class="adminSub">Form tạo key chỉ hiện sau khi nhập đúng admin key. Thống kê hôm nay tách riêng Android/iOS và tách riêng key free với key admin tạo.</div>
          </div>
        </div>
      </div>
      <div class="content">
        <div class="adminStack">
          <div id="adminLogin" class="panelBlock loginCard">
            <div class="panelTitle">Nhập admin key để mở dashboard</div>
            <div class="panelHint">Phần tạo key và danh sách key không hiện trước khi xác thực. Phiên admin chỉ lưu tạm trong trình duyệt hiện tại.</div>
            <input id="adminKeyInput" class="input" type="password" placeholder="Nhập admin key">
            <div class="formRow" style="margin-top:12px">
              <button id="adminLoginBtn" type="button" class="btn" style="margin-top:0">Vào dashboard</button>
              <button id="adminClearBtn" type="button" class="miniBtn">Xóa phiên</button>
            </div>
            <div id="authMsg" class="msg" style="margin-top:12px"></div>
            <div id="authDebug" class="debugBox"></div>
          </div>

          <div id="adminDash" class="adminHidden">
            <div class="panelBlock">
              <div class="toolbarLine">
                <div class="toolbarBadge" id="activeDay">Hôm nay</div>
                <div class="adminActions">
                  <button id="adminRefreshBtn" type="button" class="miniBtn">Làm mới thống kê</button>
                  <button id="adminListTopBtn" type="button" class="miniBtn">Làm mới danh sách</button>
                  <button id="adminLogoutBtn" type="button" class="miniBtn">Đăng xuất admin</button>
                </div>
              </div>
              <div id="statsGrid" class="statsGrid"></div>
            </div>

            <div class="adminGrid">
              <div class="panelBlock">
                <div class="panelTitle">Tạo key admin</div>
                <div class="panelHint">Key tạo ở đây đi vào luồng admin cũ. Nếu để trống ô key, hệ thống tự random.</div>
                <input id="customKey" class="input" placeholder="Key muốn tạo (để trống = tự random)">
                <div class="grid2" style="margin-top:10px">
                  <input id="uses" class="input" type="number" value="1" min="1" placeholder="Số thiết bị tối đa">
                  <input id="days" class="input" type="number" value="30" min="1" placeholder="Số ngày sử dụng">
                </div>
                <div class="formRow" style="margin-top:12px">
                  <button id="createKeyBtn" type="button" class="btn" style="margin-top:0">Tạo key</button>
                  <button id="listKeyBtn" type="button" class="miniBtn">Tải danh sách key</button>
                </div>
                <div id="result" class="msg" style="margin-top:12px"></div>
              </div>

              <div class="panelBlock">
                <div class="panelTitle">Tìm key</div>
                <div class="panelHint">Có thể tìm key admin, key free hoặc key đã bị xóa tự động do hết hạn.</div>
                <div class="searchRow">
                  <input id="findKeyInput" class="input" placeholder="Nhập key cần tìm">
                  <button id="findKeyBtn" type="button" class="miniBtn">Tìm key</button>
                </div>
                <div id="findResult" style="margin-top:12px"></div>
              </div>
            </div>

            <div class="panelBlock">
              <div class="toolbarLine">
                <div>
                  <div class="panelTitle" style="margin-bottom:6px">Danh sách key admin</div>
                  <div class="panelHint" style="margin:0">Chỉ hiển thị key do admin tạo, không lẫn key free.</div>
                </div>
                <div class="toolbarBadge" id="listCount">0 key</div>
              </div>
              <div id="list" class="listWrap"></div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <script>
    var currentAdminKey = "";
    var buildTag = "${APP_BUILD_TAG}";

    function escapeHtml(value) {
      return String(value || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function setTextMessage(elementId, type, text) {
      var box = document.getElementById(elementId);
      if (!box) return;
      if (!text) {
        box.innerHTML = "";
        return;
      }
      box.innerHTML = '<span class="' + type + '">' + escapeHtml(text) + '</span>';
    }

    function setDebugLines(lines) {
      var box = document.getElementById("authDebug");
      if (!box) return;
      var arr = Array.isArray(lines) ? lines : [lines];
      box.innerHTML = arr
        .filter(Boolean)
        .map(function (line) {
          return '<div class="debugLine">' + escapeHtml(line) + '</div>';
        })
        .join("");
    }

    function nowLabel() {
      try {
        return new Date().toLocaleString("vi-VN");
      } catch (err) {
        return String(Date.now());
      }
    }

    function getAdminHeaders(extra) {
      var headers = extra || {};
      headers["x-admin-key"] = currentAdminKey;
      return headers;
    }

    function labelScope(scope) {
      return scope === "free" ? "Key free" : "Key admin";
    }

    function labelStatus(status) {
      if (status === "active") return "Đang hoạt động";
      if (status === "used_up") return "Hết lượt nhập";
      if (status === "expired") return "Đã hết hạn";
      if (status === "expired_deleted") return "Đã hết hạn và đã tự xóa";
      return "Không rõ";
    }

    function showLoginOnly() {
      document.getElementById("adminLogin").classList.remove("adminHidden");
      document.getElementById("adminDash").classList.add("adminHidden");
    }

    function showDashboardOnly() {
      document.getElementById("adminLogin").classList.add("adminHidden");
      document.getElementById("adminDash").classList.remove("adminHidden");
    }

    function renderOverview(data) {
      var freeToday = data && data.freeToday ? data.freeToday : { day: "-", verified: 0, issued: 0, verifiedDevices: 0, issuedDevices: 0 };
      document.getElementById("activeDay").textContent = "Hôm nay " + freeToday.day;
      var counts = data && data.counts ? data.counts : { adminKeys: 0, freeKeys: 0, expiredDeletedAdminKeys: 0 };
      var stats = [
        { title: "Máy vượt link hôm nay", value: freeToday.verifiedDevices, meta: "Số máy callback Link4m hợp lệ trong ngày" },
        { title: "Máy đã lấy key hôm nay", value: freeToday.issuedDevices, meta: "Số máy đã được tạo key free trong ngày" },
        { title: "Lượt verified hôm nay", value: freeToday.verified, meta: "Tổng callback hợp lệ từ Link4m" },
        { title: "Lượt issued hôm nay", value: freeToday.issued, meta: "Tổng lần server tạo key free" },
        { title: "Tổng key admin", value: counts.adminKeys, meta: "Đang còn trong keys.json" },
        { title: "Tổng key free", value: counts.freeKeys, meta: "Đang còn trong free_keys.json" },
        { title: "Key admin đã tự xóa", value: counts.expiredDeletedAdminKeys, meta: "Marker key admin hết hạn gần đây" }
      ];
      var html = "";
      for (var i = 0; i < stats.length; i += 1) {
        var item = stats[i];
        html +=
          '<div class="statCard">' +
            '<div class="statLabel">' + escapeHtml(item.title) + '</div>' +
            '<div class="statValue">' + escapeHtml(item.value) + '</div>' +
            '<div class="statMeta">' + escapeHtml(item.meta) + '</div>' +
          '</div>';
      }
      document.getElementById("statsGrid").innerHTML = html;
    }

    function renderFindResult(item) {
      var box = document.getElementById("findResult");
      if (!item) {
        box.innerHTML = '<div class="keyRow"><div class="keyMeta">Không tìm thấy key.</div></div>';
        return;
      }
      box.innerHTML =
        '<div class="keyRow">' +
          '<div class="keyRowTop">' +
            '<div>' +
              '<div class="keyCode">' + escapeHtml(item.key) + '</div>' +
              '<div class="keyMeta">' +
                '<div><b>Nguồn:</b> ' + escapeHtml(labelScope(item.scope)) + '</div>' +
                '<div><b>Trạng thái:</b> ' + escapeHtml(labelStatus(item.status)) + '</div>' +
                '<div><b>Hết hạn:</b> ' + escapeHtml(item.expireText || "-") + '</div>' +
                '<div><b>Thiết bị:</b> ' + escapeHtml(item.usedDevices) + ' / ' + escapeHtml(item.totalDevices) + '</div>' +
                '<div><b>Lượt còn:</b> ' + escapeHtml(item.usesLeft) + '</div>' +
              '</div>' +
            '</div>' +
            '<div class="keySource">' + escapeHtml(labelScope(item.scope)) + '</div>' +
          '</div>' +
        '</div>';
    }

    async function dangNhapAdmin(savedValue) {
      var input = document.getElementById("adminKeyInput");
      var adminKey = String(savedValue || input.value || "").trim();
      if (!adminKey) {
        setTextMessage("authMsg", "err", "Vui lòng nhập admin key");
        setDebugLines([
          "Build hiện tại: " + buildTag,
          "Chưa nhập admin key nên chưa gọi API."
        ]);
        showLoginOnly();
        return;
      }
      setTextMessage("authMsg", "ok", "Đang xác thực admin key...");
      setDebugLines([
        "Build hiện tại: " + buildTag,
        "Đã bấm nút lúc: " + nowLabel(),
        "Đang gọi GET /api/admin/auth"
      ]);
      try {
        var authRes = await fetch("/api/admin/auth", {
          headers: {
            "x-admin-key": adminKey
          }
        });
        var authData = await authRes.json();
        setDebugLines([
          "Build hiện tại: " + buildTag,
          "Đã bấm nút lúc: " + nowLabel(),
          "Đã gọi GET /api/admin/auth",
          "HTTP status: " + authRes.status,
          "Kết quả API: " + (authData && authData.ok ? "ok" : (authData && authData.error ? authData.error : "không rõ"))
        ]);
        if (!authData.ok) {
          currentAdminKey = "";
          sessionStorage.removeItem("aim_admin_key");
          showLoginOnly();
          setTextMessage("authMsg", "err", authData.error || "Sai admin key");
          return;
        }
        currentAdminKey = adminKey;
        input.value = adminKey;
        sessionStorage.setItem("aim_admin_key", adminKey);
        setTextMessage("authMsg", "ok", "Đăng nhập admin thành công");
        showDashboardOnly();
        await taiTongQuan();
        await taiDanhSach();
      } catch (err) {
        showLoginOnly();
        setTextMessage("authMsg", "err", "Không kết nối được server");
        setDebugLines([
          "Build hiện tại: " + buildTag,
          "Đã bấm nút lúc: " + nowLabel(),
          "Gọi GET /api/admin/auth thất bại",
          "Chi tiết: " + (err && err.message ? err.message : "Không rõ lỗi")
        ]);
      }
    }

    function xoaPhienAdmin() {
      currentAdminKey = "";
      sessionStorage.removeItem("aim_admin_key");
      document.getElementById("adminKeyInput").value = "";
      document.getElementById("findKeyInput").value = "";
      document.getElementById("statsGrid").innerHTML = "";
      document.getElementById("list").innerHTML = "";
      document.getElementById("findResult").innerHTML = "";
      document.getElementById("listCount").textContent = "0 key";
      setTextMessage("result", "", "");
      setTextMessage("authMsg", "ok", "Đã xóa phiên admin trên trình duyệt này");
      showLoginOnly();
    }

    async function taiTongQuan() {
      if (!currentAdminKey) {
        showLoginOnly();
        return;
      }
      try {
        var res = await fetch("/api/admin/overview", {
          headers: getAdminHeaders({})
        });
        var data = await res.json();
        if (!data.ok) {
          xoaPhienAdmin();
          setTextMessage("authMsg", "err", data.error || "Sai admin key");
          return;
        }
        renderOverview(data);
      } catch (err) {
        setTextMessage("result", "err", "Không tải được thống kê");
      }
    }

    async function taoKey() {
      var customKey = document.getElementById("customKey").value.trim();
      var uses = Number(document.getElementById("uses").value || 1);
      var days = Number(document.getElementById("days").value || 30);
      var result = document.getElementById("result");
      if (!currentAdminKey) {
        setTextMessage("result", "err", "Chưa đăng nhập admin");
        showLoginOnly();
        return;
      }
      result.innerHTML = '<span class="ok">Đang tạo key...</span>';
      try {
        var res = await fetch("/api/create", {
          method: "POST",
          headers: getAdminHeaders({
            "Content-Type": "application/json"
          }),
          body: JSON.stringify({ key: customKey, uses: uses, days: days })
        });
        var data = await res.json();
        if (!data.ok) {
          result.innerHTML = '<span class="err">Lỗi: ' + escapeHtml(data.error || "Tạo key thất bại") + '</span>';
          return;
        }
        result.innerHTML =
          '<span class="ok">Tạo thành công</span><br>' +
          'Key: <b>' + escapeHtml(data.key) + '</b><br>' +
          'Số thiết bị tối đa: ' + escapeHtml(data.totalDevices) + '<br>' +
          'Hết hạn: ' + escapeHtml(data.expireText);
        document.getElementById("customKey").value = "";
        await taiTongQuan();
        await taiDanhSach();
      } catch (err) {
        result.innerHTML = '<span class="err">Lỗi mạng</span>';
      }
    }

    async function taiDanhSach() {
      if (!currentAdminKey) {
        showLoginOnly();
        return;
      }
      var box = document.getElementById("list");
      box.innerHTML = "Đang tải...";
      try {
        var res = await fetch("/api/list", {
          headers: getAdminHeaders({})
        });
        var data = await res.json();
        if (!data.ok) {
          box.innerHTML = '<span class="err">Lỗi: ' + (data.error || "Không tải được") + '</span>';
          return;
        }
        var entries = data.items || [];
        document.getElementById("listCount").textContent = String(data.count || entries.length) + " key";
        if (!entries.length) {
          box.innerHTML = "Chưa có key nào.";
          return;
        }
        var html = "";
        for (var i = 0; i < entries.length; i += 1) {
          var v = entries[i];
          html +=
            '<div class="keyRow">' +
              '<div class="keyRowTop">' +
                '<div>' +
                  '<div class="keyCode">' + escapeHtml(v.key) + '</div>' +
                  '<div class="keyMeta">' +
                    '<div><b>Lượt còn:</b> ' + escapeHtml(v.usesLeft) + '</div>' +
                    '<div><b>Thiết bị:</b> ' + escapeHtml(v.usedDevices) + ' / ' + escapeHtml(v.totalDevices) + '</div>' +
                    '<div><b>Hết hạn:</b> ' + escapeHtml(v.expireText) + '</div>' +
                  '</div>' +
                '</div>' +
                '<div class="adminActions">' +
                  '<button class="miniBtn" onclick="timKeyTheoGiaTri(\'' + v.key.replace(/\\/g, "\\\\").replace(/'/g, "\\'") + '\')">Xem</button>' +
                  '<button class="miniBtn" style="background:rgba(122,23,52,0.28);border-color:rgba(255,125,162,0.22)" onclick="xoaKey(\'' + v.key.replace(/\\/g, "\\\\").replace(/'/g, "\\'") + '\')">Xóa</button>' +
                '</div>' +
              '</div>' +
            '</div>';
        }
        box.innerHTML = html;
      } catch (err) {
        box.innerHTML = '<span class="err">Lỗi mạng</span>';
      }
    }

    async function timKey() {
      var key = document.getElementById("findKeyInput").value.trim();
      if (!key) {
        setTextMessage("findResult", "err", "Vui lòng nhập key cần tìm");
        return;
      }
      await timKeyTheoGiaTri(key);
    }

    async function timKeyTheoGiaTri(key) {
      if (!currentAdminKey) {
        showLoginOnly();
        return;
      }
      document.getElementById("findKeyInput").value = key;
      var box = document.getElementById("findResult");
      box.innerHTML = '<div class="keyRow"><div class="keyMeta">Đang tìm key...</div></div>';
      try {
        var res = await fetch("/api/admin/find?key=" + encodeURIComponent(key), {
          headers: getAdminHeaders({})
        });
        var data = await res.json();
        if (!data.ok) {
          box.innerHTML = '<div class="keyRow"><div class="keyMeta err">' + escapeHtml(data.error || "Không tìm được key") + '</div></div>';
          return;
        }
        renderFindResult(data.item);
      } catch (err) {
        box.innerHTML = '<div class="keyRow"><div class="keyMeta err">Lỗi mạng</div></div>';
      }
    }

    async function xoaKey(key) {
      if (!currentAdminKey) {
        showLoginOnly();
        return;
      }
      if (!window.confirm("Xóa key này khỏi hệ admin?")) {
        return;
      }
      var res = await fetch("/api/delete", {
        method: "POST",
        headers: getAdminHeaders({
          "Content-Type": "application/json"
        }),
        body: JSON.stringify({ key: key })
      });
      var data = await res.json();
      if (!data.ok) {
        setTextMessage("result", "err", data.error || "Xóa key thất bại");
        return;
      }
      setTextMessage("result", "ok", data.msg || "Đã xóa key");
      await taiTongQuan();
      await taiDanhSach();
    }

    function initAdminPage() {
      showLoginOnly();
      setDebugLines([
        "Trang admin đã tải xong.",
        "Build hiện tại: " + buildTag,
        "Nếu bạn vừa sửa code mà dòng build này không đổi, nghĩa là server chưa restart hoặc chưa deploy."
      ]);
      var saved = sessionStorage.getItem("aim_admin_key");
      var adminLoginBtn = document.getElementById("adminLoginBtn");
      var adminClearBtn = document.getElementById("adminClearBtn");
      var adminRefreshBtn = document.getElementById("adminRefreshBtn");
      var adminListTopBtn = document.getElementById("adminListTopBtn");
      var adminLogoutBtn = document.getElementById("adminLogoutBtn");
      var createKeyBtn = document.getElementById("createKeyBtn");
      var listKeyBtn = document.getElementById("listKeyBtn");
      var findKeyBtn = document.getElementById("findKeyBtn");
      var adminKeyInput = document.getElementById("adminKeyInput");
      var findKeyInput = document.getElementById("findKeyInput");
      if (!adminLoginBtn || !adminClearBtn || !adminRefreshBtn || !adminListTopBtn || !adminLogoutBtn || !createKeyBtn || !listKeyBtn || !findKeyBtn || !adminKeyInput || !findKeyInput) {
        setDebugLines([
          "Trang admin đã tải xong.",
          "Build hiện tại: " + buildTag,
          "Lỗi khởi tạo: không tìm thấy đủ phần tử HTML để gắn sự kiện."
        ]);
        return;
      }
      adminLoginBtn.addEventListener("click", function () {
        dangNhapAdmin();
      });
      adminClearBtn.addEventListener("click", function () {
        xoaPhienAdmin();
      });
      adminRefreshBtn.addEventListener("click", function () {
        taiTongQuan();
      });
      adminListTopBtn.addEventListener("click", function () {
        taiDanhSach();
      });
      adminLogoutBtn.addEventListener("click", function () {
        xoaPhienAdmin();
      });
      createKeyBtn.addEventListener("click", function () {
        taoKey();
      });
      listKeyBtn.addEventListener("click", function () {
        taiDanhSach();
      });
      findKeyBtn.addEventListener("click", function () {
        timKey();
      });
      if (saved) {
        adminKeyInput.value = saved;
        dangNhapAdmin(saved);
      }
      adminKeyInput.addEventListener("keydown", function (event) {
        if (event.key === "Enter") dangNhapAdmin();
      });
      findKeyInput.addEventListener("keydown", function (event) {
        if (event.key === "Enter") timKey();
      });
    }

    try {
      if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", initAdminPage, { once: true });
      } else {
        initAdminPage();
      }
    } catch (err) {
      setDebugLines([
        "Build hiện tại: " + buildTag,
        "Lỗi khởi tạo script admin.",
        "Chi tiết: " + (err && err.message ? err.message : "Không rõ lỗi")
      ]);
    }
  </script>
</body>
</html>
  `;
}

function buildAdminViewState(searchKey = "") {
  syncExpiredMainKeysInBackground();
  const freeToday = getFreeTodaySummary();
  const adminItems = getAdminKeyItems();
  const freeItems = getFreeKeyItems();
  const normalizedSearchKey = String(searchKey || "").trim();
  return {
    freeToday,
    adminItems,
    counts: {
      adminKeys: adminItems.length,
      freeKeys: freeItems.length,
      expiredDeletedAdminKeys: Object.keys(expiredMainKeys || {}).length
    },
    searchKey: normalizedSearchKey,
    searchItem: normalizedSearchKey ? findKeySummary(normalizedSearchKey) : null
  };
}

function renderAdminAlert(type, text) {
  if (!text) return "";
  return `<div class="adminAlert ${type === "error" ? "adminAlertErr" : "adminAlertOk"}">${escapeHtml(text)}</div>`;
}

function renderAdminSearchResult(item, searchKey) {
  if (!searchKey) return "";
  if (!item) {
    return `<div class="keyRow"><div class="keyMeta">Không tìm thấy key: <b>${escapeHtml(searchKey)}</b></div></div>`;
  }
  const scopeLabel = item.scope === "free" ? "Key free" : "Key admin";
  const statusLabel =
    item.status === "active"
      ? "Đang hoạt động"
      : item.status === "used_up"
        ? "Hết lượt nhập"
        : item.status === "expired"
          ? "Đã hết hạn"
          : item.status === "expired_deleted"
            ? "Đã hết hạn và đã tự xóa"
            : "Không rõ";
  return `
    <div class="keyRow">
      <div class="keyRowTop">
        <div>
          <div class="keyCode">${escapeHtml(item.key)}</div>
          <div class="keyMeta">
            <div><b>Nguồn:</b> ${escapeHtml(scopeLabel)}</div>
            <div><b>Trạng thái:</b> ${escapeHtml(statusLabel)}</div>
            <div><b>Hết hạn:</b> ${escapeHtml(item.expireText || "-")}</div>
            <div><b>Thiết bị:</b> ${escapeHtml(item.usedDevices)} / ${escapeHtml(item.totalDevices)}</div>
            <div><b>Lượt còn:</b> ${escapeHtml(item.usesLeft)}</div>
          </div>
        </div>
        <div class="keySource">${escapeHtml(scopeLabel)}</div>
      </div>
    </div>
  `;
}

function renderAdminHtmlServer(options = {}) {
  const authenticated = !!options.authenticated;
  const freeToday = options.freeToday || {
    day: getVNDayKey(),
    verified: 0,
    issued: 0,
    verifiedDevices: 0,
    issuedDevices: 0
  };
  const counts = options.counts || {
    adminKeys: 0,
    freeKeys: 0,
    expiredDeletedAdminKeys: 0
  };
  const adminItems = Array.isArray(options.adminItems) ? options.adminItems : [];
  const searchKey = String(options.searchKey || "").trim();
  const searchItem = options.searchItem || null;
  const stats = [
    ["Máy vượt link hôm nay", freeToday.verifiedDevices, "Số máy callback Link4m hợp lệ trong ngày"],
    ["Máy đã lấy key hôm nay", freeToday.issuedDevices, "Số máy đã được tạo key free trong ngày"],
    ["Lượt verified hôm nay", freeToday.verified, "Tổng callback hợp lệ từ Link4m"],
    ["Lượt issued hôm nay", freeToday.issued, "Tổng lần server tạo key free"],
    ["Tổng key admin", counts.adminKeys, "Đang còn trong keys.json"],
    ["Tổng key free", counts.freeKeys, "Đang còn trong free_keys.json"],
    ["Key admin đã tự xóa", counts.expiredDeletedAdminKeys, "Marker key admin hết hạn gần đây"]
  ];
  const statsHtml = stats
    .map(
      ([label, value, meta]) => `
        <div class="statCard">
          <div class="statLabel">${escapeHtml(label)}</div>
          <div class="statValue">${escapeHtml(value)}</div>
          <div class="statMeta">${escapeHtml(meta)}</div>
        </div>
      `
    )
    .join("");
  const createdKeyBlock = options.createdKey
    ? `
      <div class="keyRow">
        <div class="keyCode">${escapeHtml(options.createdKey.key)}</div>
        <div class="keyMeta">
          <div><b>Số thiết bị tối đa:</b> ${escapeHtml(options.createdKey.totalDevices)}</div>
          <div><b>Hết hạn:</b> ${escapeHtml(options.createdKey.expireText)}</div>
        </div>
      </div>
    `
    : "";
  const listHtml = adminItems.length
    ? adminItems
        .map(
          (item) => `
            <div class="keyRow">
              <div class="keyRowTop">
                <div>
                  <div class="keyCode">${escapeHtml(item.key)}</div>
                  <div class="keyMeta">
                    <div><b>Lượt còn:</b> ${escapeHtml(item.usesLeft)}</div>
                    <div><b>Thiết bị:</b> ${escapeHtml(item.usedDevices)} / ${escapeHtml(item.totalDevices)}</div>
                    <div><b>Hết hạn:</b> ${escapeHtml(item.expireText)}</div>
                  </div>
                </div>
                <div class="adminActions">
                  <form method="get" action="/${ADMIN_ROUTE}">
                    <input type="hidden" name="find" value="${escapeHtml(item.key)}">
                    <button class="miniBtn" type="submit">Xem</button>
                  </form>
                  <form method="post" action="/${ADMIN_ROUTE}/delete" onsubmit="return confirm('Xóa key này khỏi hệ admin?')">
                    <input type="hidden" name="key" value="${escapeHtml(item.key)}">
                    <button class="miniBtn miniBtnDanger" type="submit">Xóa</button>
                  </form>
                </div>
              </div>
            </div>
          `
        )
        .join("")
    : `<div class="keyRow"><div class="keyMeta">Chưa có key admin nào.</div></div>`;
  return `
<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">
  <meta name="theme-color" content="#05040b">
  <title>Admin Dashboard</title>
  ${baseStyles()}
  <style>
    .adminWrap{width:min(1120px,100%);margin:0 auto}
    .adminStack{display:grid;gap:16px}
    .adminEyebrow{font-size:12px;letter-spacing:.18em;text-transform:uppercase;color:#d6c28f;margin-bottom:8px}
    .adminHeading{margin:0;font-size:clamp(28px,4vw,42px);line-height:1.05}
    .adminSub{margin-top:10px;color:var(--muted);font-size:15px;line-height:1.7;max-width:760px}
    .panelBlock{border:1px solid rgba(255,255,255,.08);border-radius:24px;padding:20px;background:linear-gradient(180deg, rgba(255,255,255,.03), rgba(255,255,255,.015)), rgba(8,8,14,.84);box-shadow:0 18px 46px rgba(0,0,0,.28)}
    .panelTitle{margin:0 0 14px;font-size:21px;font-weight:800}
    .panelHint{color:var(--muted);font-size:14px;line-height:1.7;margin-bottom:14px}
    .adminGrid{display:grid;gap:16px;grid-template-columns:1.08fr .92fr}
    .statsGrid{display:grid;gap:14px;grid-template-columns:repeat(4,minmax(0,1fr))}
    .statCard{border:1px solid rgba(255,255,255,.08);border-radius:22px;padding:18px;background:rgba(255,255,255,.025);min-height:124px}
    .statLabel{color:var(--muted);font-size:13px;margin-bottom:10px;line-height:1.5}
    .statValue{font-size:clamp(24px,3vw,36px);font-weight:900;line-height:1;margin-bottom:8px}
    .statMeta{font-size:13px;color:#f3dfaa;line-height:1.5}
    .toolbarLine{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:14px}
    .toolbarBadge{display:inline-flex;align-items:center;gap:8px;padding:10px 14px;border-radius:999px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.03);font-size:13px;color:var(--muted)}
    .adminActions{display:flex;gap:10px;flex-wrap:wrap}
    .miniBtn{height:48px;padding:0 18px;border-radius:16px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.04);color:var(--text);font-size:14px;font-weight:700;cursor:pointer}
    .miniBtnDanger{background:rgba(122,23,52,.28);border-color:rgba(255,125,162,.22)}
    .searchRow,.formRow{display:grid;gap:10px;grid-template-columns:1fr auto}
    .listWrap{display:grid;gap:12px}
    .keyRow{border:1px solid rgba(255,255,255,.08);border-radius:20px;padding:16px;background:rgba(255,255,255,.022)}
    .keyRowTop{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap}
    .keyCode{font-size:16px;font-weight:800;word-break:break-all;margin-bottom:10px}
    .keyMeta{display:grid;gap:6px;color:var(--muted);font-size:14px;line-height:1.6}
    .keySource{display:inline-flex;align-items:center;padding:8px 12px;border-radius:999px;background:rgba(214,194,143,.14);border:1px solid rgba(214,194,143,.2);color:#f3dfaa;font-size:12px;font-weight:800;letter-spacing:.08em;text-transform:uppercase}
    .loginCard{max-width:580px;margin:0 auto}
    .adminAlert{margin-top:12px;padding:12px 14px;border-radius:16px;border:1px solid rgba(255,255,255,.08);font-size:14px;line-height:1.6}
    .adminAlertOk{background:rgba(72,165,106,.12);color:#d4f0de;border-color:rgba(122,214,155,.22)}
    .adminAlertErr{background:rgba(182,62,98,.14);color:#ffd0dc;border-color:rgba(255,125,162,.22)}
    .buildText{margin-top:10px;color:#a99dbf;font-size:12px;line-height:1.6}
    @media (max-width: 980px){.statsGrid{grid-template-columns:repeat(2,minmax(0,1fr))}.adminGrid{grid-template-columns:1fr}}
    @media (max-width: 720px){.panelBlock{padding:18px;border-radius:22px}.searchRow,.formRow,.statsGrid{grid-template-columns:1fr}.miniBtn,.btn,.smallBtn{width:100%}}
  </style>
</head>
<body>
  <div class="bgAura"><div class="orb o1"></div><div class="orb o2"></div><div class="orb o3"></div></div>
  <div class="wrap">
    <div class="card adminWrap">
      <div class="top">
        <div class="brand" style="width:100%;justify-content:space-between;gap:18px;flex-wrap:wrap">
          <div class="logoBox">${renderLogo(74,22)}</div>
          <div>
            <div class="adminEyebrow">Admin Route</div>
            <h1 class="adminHeading">Dashboard quản lý key</h1>
            <div class="adminSub">Admin route này chạy bằng form server-side, không phụ thuộc JavaScript của trình duyệt.</div>
            <div class="buildText">Build: ${escapeHtml(APP_BUILD_TAG)}</div>
          </div>
        </div>
      </div>
      <div class="content">
        <div class="adminStack">
          ${
            !authenticated
              ? `
                <div class="panelBlock loginCard">
                  <div class="panelTitle">Nhập admin key để mở dashboard</div>
                  <div class="panelHint">Form tạo key và danh sách key chỉ hiện sau khi xác thực thành công.</div>
                  <form method="post" action="/${ADMIN_ROUTE}/login">
                    <input name="adminKey" class="input" type="password" placeholder="Nhập admin key" autocomplete="current-password">
                    <button class="btn" type="submit">Vào dashboard</button>
                  </form>
                  ${renderAdminAlert("error", options.error)}
                  ${renderAdminAlert("success", options.success)}
                </div>
              `
              : `
                <div class="panelBlock">
                  <div class="toolbarLine">
<div class="toolbarBadge">Hôm nay ${escapeHtml(freeToday.day)}</div>
                    <div class="adminActions">
                      <form method="get" action="/${ADMIN_ROUTE}">
                        <button class="miniBtn" type="submit">Làm mới dashboard</button>
                      </form>
                      <form method="post" action="/${ADMIN_ROUTE}/logout">
                        <button class="miniBtn" type="submit">Đăng xuất admin</button>
                      </form>
                    </div>
                  </div>
                  ${renderAdminAlert("error", options.error)}
                  ${renderAdminAlert("success", options.success)}
                  ${createdKeyBlock}
                  <div class="statsGrid">${statsHtml}</div>
                </div>
                <div class="adminGrid">
                  <div class="panelBlock">
                    <div class="panelTitle">Tạo key admin</div>
                    <div class="panelHint">Nếu để trống ô key, hệ thống tự random.</div>
                    <form method="post" action="/${ADMIN_ROUTE}/create">
                      <input name="key" class="input" placeholder="Key muốn tạo (để trống = tự random)">
                      <div class="grid2" style="margin-top:10px">
                        <input name="uses" class="input" type="number" value="1" min="1" placeholder="Số thiết bị tối đa">
                        <input name="days" class="input" type="number" value="30" min="1" placeholder="Số ngày sử dụng">
                      </div>
                      <button class="btn" type="submit">Tạo key</button>
                    </form>
                  </div>
                  <div class="panelBlock">
                    <div class="panelTitle">Tìm key</div>
                    <div class="panelHint">Tìm key admin, key free hoặc key đã tự xóa do hết hạn.</div>
                    <form method="get" action="/${ADMIN_ROUTE}">
                      <div class="searchRow">
                        <input name="find" class="input" value="${escapeHtml(searchKey)}" placeholder="Nhập key cần tìm">
                        <button class="miniBtn" type="submit">Tìm key</button>
                      </div>
                    </form>
                    <div style="margin-top:12px">${renderAdminSearchResult(searchItem, searchKey)}</div>
                  </div>
                </div>
                <div class="panelBlock">
                  <div class="toolbarLine">
                    <div>
                      <div class="panelTitle" style="margin-bottom:6px">Danh sách key admin</div>
                      <div class="panelHint" style="margin:0">Chỉ hiển thị key do admin tạo, không lẫn key free.</div>
                    </div>
                    <div class="toolbarBadge">${escapeHtml(adminItems.length)} key</div>
                  </div>
                  <div class="listWrap">${listHtml}</div>
                </div>
              `
          }
        </div>
      </div>
    </div>
  </div>
</body>
</html>
  `;
}

const freeModule = registerFreeKeyRoutes(app, {
  rootDir: __dirname,
  sessionSecret: SESSION_SECRET,
  baseStyles,
  renderLogo,
  genKey,
  formatVNTime,
  normalizeKeyItem,
  normalizeAllStore,
  getKeys: () => keys,
  getAllKeys: getAllKeysSnapshot,
  saveKeys: saveStore,
  hasGithubStore,
  readGithubStore,
  writeGithubStore,
  isAdmin,
  vnTimezone: VN_TIMEZONE,
  freeRuntime,
  mainStoreConfig,
  freeStoreConfig
});

app.get("/healthz", async (req, res) => {
  const out = {
    ok: true,
    storeMode: getStoreMode(),
    githubConfigured: hasGithubStore(),
    mainStore: {
      mode: getStoreMode(mainStoreConfig),
      repo: mainStoreConfig.repo || "",
      branch: mainStoreConfig.branch || ""
    },
    keyCount: Object.keys(keys || {}).length,
    remoteKeyCount: null,
    remoteReadOk: false
  };

  if (hasGithubStore(mainStoreConfig)) {
    try {
      const remote = await readGithubStore(mainStoreConfig.dataPath, mainStoreConfig);
      out.remoteReadOk = true;
      out.remoteKeyCount = Object.keys(remote || {}).length;
    } catch (err) {
      out.remoteReadOk = false;
      out.remoteError = err.message;
      out.remoteStatus = err.statusCode || null;
    }
  }

  if (freeRuntime && typeof freeRuntime.getHealth === "function") {
    out.free = freeRuntime.getHealth();
  }

  res.json(out);
});

app.get("/", (req, res) => {
  res.send(renderHomeHtml());
});

app.get("/panel", (req, res) => {
  if (shouldShowAndroidUpdate(req)) {
    return res.send(renderAndroidUpdateHtml());
  }
  res.send(renderPanelHtml());
});

app.get("/" + ADMIN_ROUTE, (req, res) => {
  if (!hasAdminSession(req)) {
    return res.send(
      renderAdminHtmlServer({
        authenticated: false,
        error: req.query.error || "",
        success: req.query.success || ""
      })
    );
  }
  const state = buildAdminViewState(req.query.find || "");
  return res.send(
    renderAdminHtmlServer({
      authenticated: true,
      ...state,
      error: req.query.error || "",
      success: req.query.success || ""
    })
  );
});

app.post("/" + ADMIN_ROUTE + "/login", (req, res) => {
  const adminKey = String(req.body.adminKey || "").trim();
  if (!ADMIN_KEY) {
    return res.send(
      renderAdminHtmlServer({
        authenticated: false,
        error: "ADMIN_PASSWORD chưa được cấu hình trong env"
      })
    );
  }
  if (adminKey !== ADMIN_KEY) {
    return res.send(
      renderAdminHtmlServer({
        authenticated: false,
        error: "Sai admin key"
      })
    );
  }
  setAdminSessionCookie(res);
  const state = buildAdminViewState();
  return res.send(
    renderAdminHtmlServer({
      authenticated: true,
      ...state,
      success: "Đăng nhập admin thành công"
    })
  );
});

app.post("/" + ADMIN_ROUTE + "/logout", (req, res) => {
  clearAdminSessionCookie(res);
  return res.send(
    renderAdminHtmlServer({
      authenticated: false,
      success: "Đã đăng xuất admin"
    })
  );
});

app.post("/" + ADMIN_ROUTE + "/create", async (req, res) => {
  if (!hasAdminSession(req)) {
    return res.send(
      renderAdminHtmlServer({
        authenticated: false,
        error: "Phiên admin đã hết hoặc chưa đăng nhập"
      })
    );
  }

  const customKey = String(req.body.key || "").trim();
  const totalDevices = Math.max(1, Number(req.body.uses || 1));
  const days = Math.max(1, Number(req.body.days || 30));
  const key = customKey || genKey();
  if (findKeyRecord(key)) {
    const state = buildAdminViewState();
    return res.send(
      renderAdminHtmlServer({
        authenticated: true,
        ...state,
        error: "Key đã tồn tại"
      })
    );
  }
  const expireAt =
    days >= PERMANENT_KEY_DAYS ? 0 : Date.now() + days * 24 * 60 * 60 * 1000;

  keys[key] = {
    usesLeft: totalDevices,
    totalDevices,
    devices: [],
    expireAt,
    createdAt: Date.now()
  };
  await saveStore();
  const state = buildAdminViewState();
  return res.send(
    renderAdminHtmlServer({
      authenticated: true,
      ...state,
      success: "Tạo key admin thành công",
      createdKey: {
        key,
        totalDevices,
        expireText: formatVNTime(expireAt)
      }
    })
  );
});

app.post("/" + ADMIN_ROUTE + "/delete", async (req, res) => {
  if (!hasAdminSession(req)) {
    return res.send(
      renderAdminHtmlServer({
        authenticated: false,
        error: "Phiên admin đã hết hoặc chưa đăng nhập"
      })
    );
  }
  const key = String(req.body.key || "").trim();
  if (!keys[key]) {
    const state = buildAdminViewState();
    return res.send(
      renderAdminHtmlServer({
        authenticated: true,
        ...state,
        error: "Không tìm thấy key"
      })
    );
  }
  delete keys[key];
  await saveStore();
  const state = buildAdminViewState();
  return res.send(
    renderAdminHtmlServer({
      authenticated: true,
      ...state,
      success: "Đã xóa key admin"
    })
  );
});

app.post("/api/create", async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(401).json({ ok: false, error: "Sai admin key" });
  }

  const customKey = String(req.body.key || "").trim();
  const totalDevices = Math.max(1, Number(req.body.uses || 50));
  const days = Math.max(1, Number(req.body.days || 30));
  const key = customKey || genKey();
  if (findKeyRecord(key)) {
    return res.json({ ok: false, error: "Key đã tồn tại" });
  }
  const expireAt =
    days >= PERMANENT_KEY_DAYS ? 0 : Date.now() + days * 24 * 60 * 60 * 1000;

  keys[key] = {
    usesLeft: totalDevices,
    totalDevices: totalDevices,
    devices: [],
    expireAt: expireAt,
    createdAt: Date.now()
  };
  await saveStore();

  return res.json({
    ok: true,
    key,
    uses: totalDevices,
    totalDevices,
    expireAt,
    expireText: formatVNTime(expireAt)
  });
});

app.get("/api/check", (req, res) => {
  return res.json({ ok: true, msg: "API check hoạt động, hãy dùng POST để kiểm tra key" });
});

app.get("/api/admin/overview", async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(401).json({ ok: false, error: "Sai admin key" });
  }

  syncExpiredMainKeysInBackground();
  const freeToday = getFreeTodaySummary();
  const adminItems = getAdminKeyItems();
  const freeItems = getFreeKeyItems();

  return res.json({
    ok: true,
    freeToday,
    counts: {
      adminKeys: adminItems.length,
      freeKeys: freeItems.length,
      expiredDeletedAdminKeys: Object.keys(expiredMainKeys || {}).length
    },
    recentAdminKeys: adminItems.slice(0, 8),
    recentFreeKeys: freeItems.slice(0, 8)
  });
});

app.get("/api/admin/auth", (req, res) => {
  if (!isAdmin(req)) {
    return res.status(401).json({ ok: false, error: "Sai admin key" });
  }
  return res.json({ ok: true });
});

app.get("/api/admin/find", async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(401).json({ ok: false, error: "Sai admin key" });
  }
  syncExpiredMainKeysInBackground();
  return res.json({
    ok: true,
    item: findKeySummary(req.query.key || "")
  });
});

app.post("/api/check", async (req, res) => {
  if (shouldShowAndroidUpdate(req)) {
    return res.json({
      ok: false,
      msg:
        "Android đã update bản mới. Nếu đã mua key vĩnh viễn thì liên hệ admin app, còn anh em free vào bio TikTok Huy FanTa lấy bản mới nhất."
    });
  }
  syncExpiredMainKeysInBackground();
  const key = String(req.body.key || "").trim();
  const device = String(req.body.device || "").trim();

  if (!key || !device) {
    return res.json({ ok: false, msg: "Thiếu key hoặc thiết bị" });
  }

  const record = findKeyRecord(key);
  if (!record) {
    if (isExpiredDeletedMainKey(key)) {
      return res.json({
        ok: false,
        msg: "Key này đã hết hạn, hết lượt nhập, vui lòng xem video mới nhất"
      });
    }
    return res.json({ ok: false, msg: "Key không tồn tại" });
  }
  const item = record.item;

  if (item.expireAt && Date.now() >= item.expireAt) {
    return res.json({ ok: false, msg: "Key đã hết hạn" });
  }

  const alreadyUsed = item.devices.includes(device);
  let changed = false;

  if (!alreadyUsed) {
    if (item.usesLeft <= 0) {
      return res.json({ ok: false, msg: "Key Này Đã Hết Lượt Nhập !" });
    }
    item.devices.push(device);
    item.usesLeft -= 1;
    changed = true;
  }

  if (changed) {
    saveKeyRecord(record.scope, key, item, { background: true }).catch((err) => {
      console.error("Background save failed:", err.message);
    });
  }

  const token = createSessionToken(key, device, item.expireAt);
  return res.json({
    ok: true,
    msg: "Đăng nhập thành công",
    key,
    token,
    expireAt: item.expireAt,
    expireText: formatVNTime(item.expireAt),
    usesLeft: item.usesLeft,
    usedDevices: item.devices.length,
    totalDevices: item.totalDevices
  });
});

app.post("/api/status", async (req, res) => {
  const token = String(req.body.token || "").trim();
  const device = String(req.body.device || "").trim();

  if (!token || !device) {
    return res.json({ ok: false, msg: "Thiếu phiên đăng nhập" });
  }

  const parsed = verifySessionToken(token);
  if (!parsed) {
    return res.json({ ok: false, msg: "Phiên không hợp lệ" });
  }

  if (parsed.device !== device) {
    return res.json({ ok: false, msg: "Phiên không đúng thiết bị" });
  }

  const record = findKeyRecord(parsed.key);
  if (!record) {
    return res.json({ ok: false, msg: "Key không tồn tại" });
  }
  const item = record.item;

  if (item.expireAt && Date.now() >= item.expireAt) {
    return res.json({ ok: false, msg: "Key đã hết hạn" });
  }

  if (!item.devices.includes(device)) {
    return res.json({ ok: false, msg: "Thiết bị chưa được cấp quyền cho key này" });
  }

  return res.json({
    ok: true,
    key: parsed.key,
    expireAt: item.expireAt,
    expireText: formatVNTime(item.expireAt),
    usesLeft: item.usesLeft,
    usedDevices: item.devices.length,
    totalDevices: item.totalDevices
  });
});

app.get("/api/list", async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(401).json({ ok: false, error: "Sai admin key" });
  }

  syncExpiredMainKeysInBackground();
  const items = getAdminKeyItems();

  return res.json({ ok: true, items, count: items.length });
});

app.post("/api/delete", async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(401).json({ ok: false, error: "Sai admin key" });
  }

  const key = String(req.body.key || "").trim();
  if (!keys[key]) {
    return res.json({ ok: false, error: "Không tìm thấy key" });
  }

  delete keys[key];
  await saveStore();
  return res.json({ ok: true, msg: "Đã xóa key" });
});

Promise.all([initStore(), freeModule && freeModule.ready ? freeModule.ready : Promise.resolve()]).finally(() => {
  app.listen(PORT, () => {
    console.log("Server chạy tại port " + PORT);
    console.log("thua2");
  });
});
