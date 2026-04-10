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

  let freeStore = { claims: {}, cooldowns: {} };
  let freeReady = false;
  let freeSaveQueue = Promise.resolve();
  const freeRateMap = new Map();

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

  function ensureLocalStoreDir() {
    fs.mkdirSync(path.dirname(FREE_STORE_PATH), { recursive: true });
  }

  function appendLogLine(line) {
    try {
      fs.mkdirSync(path.dirname(FREE_LOG_PATH), { recursive: true });
      fs.appendFileSync(FREE_LOG_PATH, `${line}\n`, "utf8");
    } catch (err) {
      console.error("Write free log failed:", err.message);
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
    const keepClaims = {};
    Object.keys(freeStore.claims || {}).forEach((claimId) => {
      const claim = freeStore.claims[claimId];
      if (!claim || typeof claim !== "object") return;
      const claimedUntil = Number(claim.claimedAt || 0) + FREE_COOLDOWN_MS;
      if (claim.status === "claimed" && claimedUntil > now) {
        keepClaims[claimId] = claim;
        return;
      }
      if (Number(claim.expiresAt || 0) > now - FREE_COOLDOWN_MS) {
        keepClaims[claimId] = claim;
      }
    });

    const keepCooldowns = {};
    Object.keys(freeStore.cooldowns || {}).forEach((identityHash) => {
      const until = Number(freeStore.cooldowns[identityHash] || 0);
      if (until > now) keepCooldowns[identityHash] = until;
    });

    freeStore = { claims: keepClaims, cooldowns: keepCooldowns };
  }

  function loadLocalStore() {
    try {
      ensureLocalStoreDir();
      if (!fs.existsSync(FREE_STORE_PATH)) return { claims: {}, cooldowns: {} };
      const raw = fs.readFileSync(FREE_STORE_PATH, "utf8");
      return normalizeFreeStore(JSON.parse(raw || "{}"));
    } catch {
      return { claims: {}, cooldowns: {} };
    }
  }

  function saveLocalStore(snapshot) {
    ensureLocalStoreDir();
    fs.writeFileSync(FREE_STORE_PATH, JSON.stringify(snapshot, null, 2), "utf8");
  }

  async function initStore() {
    try {
      if (deps.hasGithubStore()) {
        freeStore = normalizeFreeStore(
          await deps.readGithubStore(FREE_GITHUB_DATA_PATH)
        );
      } else {
        freeStore = loadLocalStore();
      }
    } catch (err) {
      console.error("Free store init failed, fallback local:", err.message);
      freeStore = loadLocalStore();
    }
    cleanupFreeStore();
    freeReady = true;
  }

  async function persistFreeStore(snapshot) {
    try {
      if (deps.hasGithubStore()) {
        await deps.writeGithubStore(
          snapshot,
          FREE_GITHUB_DATA_PATH,
          "Update free claims store"
        );
      } else {
        saveLocalStore(snapshot);
      }
    } catch (err) {
      console.error("Persist free store failed:", err.message);
      saveLocalStore(snapshot);
    }
  }

  async function saveStore() {
    cleanupFreeStore();
    const snapshot = JSON.parse(JSON.stringify(freeStore));
    freeSaveQueue = freeSaveQueue
      .then(() => persistFreeStore(snapshot))
      .catch((err) => {
        console.error("Free save queue failed:", err.message);
      });
    return freeSaveQueue;
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

  function renderPage(options = {}) {
    const title = esc(options.title || "Nhận Key Free");
    const heading = esc(options.heading || "Nhận Key Free");
    const sub = esc(
      options.sub ||
        "Mua key vĩnh viễn liên hệ Zalo Admin Huy Fanta 0818 249 250"
    );
    const alertClass =
      options.alertType === "err" ? "err" : options.alertType === "ok" ? "ok" : "";
    const alertText = options.alert ? esc(options.alert) : "";
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
          <div class="sub" style="margin:0">
            1 ngày lấy được 1 key 1 ngày, video trên TikTok thì xem nhanh có key 2 ngày không vượt link nhưng chậm là hết à nhe.
          </div>
        </div>
        ${captchaMarkup}
        <button id="freeStartBtn" class="btn" style="margin-top:14px">Bắt đầu vượt Link4m</button>
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
                  button.textContent = "Bắt đầu vượt Link4m";
                  return;
                }
                status.className = "msg ok";
                status.textContent = "Đang mở Link4m...";
                window.location.href = data.redirectUrl;
              } catch (err) {
                status.className = "msg err";
                status.textContent = "Không kết nối được tới máy chủ";
                button.disabled = false;
                button.textContent = "Bắt đầu vượt Link4m";
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
            <div class="credit">Mua key vĩnh viễn liên hệ Zalo Admin Huy Fanta 0818 249 250</div>
          </div>
        </div>
      </div>
      <div class="content">
        <div class="noticeBox">
          1 ngày lấy được 1 key 1 ngày, video trên TikTok thì xem nhanh có key 2 ngày không vượt link nhưng chậm là hết à nhe.
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
    const keys = deps.getKeys();
    if (claim.key && keys[claim.key]) {
      return {
        key: claim.key,
        expireAt: Number(claim.keyExpireAt || 0)
      };
    }

    let nextKey = "";
    do {
      nextKey = deps.genKey();
    } while (keys[nextKey]);

    const expireAt = Date.now() + FREE_KEY_DAYS * 24 * 60 * 60 * 1000;
    keys[nextKey] = {
      usesLeft: FREE_KEY_USES,
      totalDevices: FREE_KEY_USES,
      devices: [],
      expireAt,
      createdAt: Date.now(),
      source: "free"
    };

    claim.status = "claimed";
    claim.verifiedAt = claim.verifiedAt || Date.now();
    claim.claimedAt = Date.now();
    claim.key = nextKey;
    claim.keyExpireAt = expireAt;
    freeStore.cooldowns[claim.identityHash] = Date.now() + FREE_COOLDOWN_MS;

    await deps.saveKeys();
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

  app.post("/api/free/start", async (req, res) => {
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
      claimId: claim.id
    });

    const issued = await issueKeyForClaim(claim);
    logEvent("claim_issued", req, {
      claimId: claim.id,
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

  initStore().catch((err) => {
    console.error("Init free key routes failed:", err.message);
  });
};
