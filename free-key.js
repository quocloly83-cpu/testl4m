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

  let freeStore = { claims: {}, cooldowns: {} };
  let freeReady = false;
  let freeSaveQueue = Promise.resolve();

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

  function requestJson(url) {
    return new Promise((resolve, reject) => {
      const target = new URL(url);
      const req = https.request(
        {
          protocol: target.protocol,
          hostname: target.hostname,
          port: target.port || undefined,
          path: `${target.pathname}${target.search}`,
          method: "GET",
          timeout: 10000,
          headers: {
            "User-Agent": "aimtrickhead-free-key",
            Accept: "application/json"
          }
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
              reject(new Error("Link4m API tra ve du lieu khong hop le."));
            }
          });
        }
      );
      req.on("error", reject);
      req.on("timeout", () => req.destroy(new Error("Link4m API timeout.")));
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
        throw new Error("Khong tao duoc link Link4m.");
      }
      return shortenedUrl;
    }

    const templatedUrl = buildLink4mUrl(targetUrl);
    if (templatedUrl) return templatedUrl;
    throw new Error("Chua cau hinh Link4m API hoac template.");
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

  function formatDuration(ms) {
    const totalMinutes = Math.max(1, Math.ceil(ms / 60000));
    if (totalMinutes < 60) return `${totalMinutes} phut`;
    const hours = Math.floor(totalMinutes / 60);
    const minutes = totalMinutes % 60;
    if (!minutes) return `${hours} gio`;
    return `${hours} gio ${minutes} phut`;
  }

  function renderPage(options = {}) {
    const title = esc(options.title || "Nhan Key Free");
    const heading = esc(options.heading || "Nhan Key Free");
    const sub = esc(
      options.sub ||
        "Vuot Link4m xong he thong se tu tao 1 key random, khong can vao panel."
    );
    const alertClass =
      options.alertType === "err" ? "err" : options.alertType === "ok" ? "ok" : "";
    const alertText = options.alert ? esc(options.alert) : "";
    const keyBlock = options.key
      ? `
        <div class="tile" style="margin-top:14px">
          <div class="sub" style="margin:0 0 8px">Key da nhan</div>
          <div id="freeKeyValue" style="font-size:30px;line-height:1.15;font-weight:700;word-break:break-word;color:#efe8ff">${esc(
            options.key
          )}</div>
          <div class="sub" style="margin-top:10px">Han dung: ${esc(
            options.expireText || ""
          )}</div>
        </div>
        <div class="grid2" style="margin-top:12px">
          <button id="copyKeyBtn" class="btn" style="margin-top:0">Copy key</button>
          <a class="smallBtn" href="/panel" style="height:56px">Mo panel</a>
        </div>
      `
      : `
        <div class="tile" style="margin-top:14px">
          <div class="sub" style="margin:0">
            Key free mac dinh: ${FREE_KEY_USES} thiet bi, ${FREE_KEY_DAYS} ngay, cooldown ${formatDuration(
              FREE_COOLDOWN_MS
            )}.
          </div>
        </div>
        <button id="freeStartBtn" class="btn" style="margin-top:14px">Bat dau vuot Link4m</button>
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
                copyBtn.textContent = "Da copy";
              } catch (err) {
                copyBtn.textContent = "Copy loi";
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
              button.textContent = "Dang chuyen...";
              status.className = "msg";
              status.textContent = "Dang tao phien vuot link...";

              try {
                var res = await fetch("/api/free/start", {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({ browserId: getBrowserId() })
                });
                var data = await res.json();
                if (!data.ok) {
                  status.className = "msg err";
                  status.textContent = data.msg || "Khong tao duoc phien";
                  button.disabled = false;
                  button.textContent = "Bat dau vuot Link4m";
                  return;
                }
                status.className = "msg ok";
                status.textContent = "Dang mo Link4m...";
                window.location.href = data.redirectUrl;
              } catch (err) {
                status.className = "msg err";
                status.textContent = "Khong ket noi duoc toi may chu";
                button.disabled = false;
                button.textContent = "Bat dau vuot Link4m";
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
  <style>
    .freeCard{max-width:640px}
    .freeMeta{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:14px}
    .freeMeta .tile{margin-top:0}
    @media (max-width:640px){.freeMeta{grid-template-columns:1fr}}
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
            <div class="credit">LINK RIENG · HE FREE TACH BIET</div>
          </div>
        </div>
      </div>
      <div class="content">
        <div class="noticeBox">
          Vuot xong Link4m, server moi tu tao key. Panel cu va route admin khong bi sua luong.
        </div>
        <div class="freeMeta">
          <div class="tile"><b>Route</b><div class="sub" style="margin-top:6px">/${esc(
            FREE_ROUTE
          )}</div></div>
          <div class="tile"><b>Cooldown</b><div class="sub" style="margin-top:6px">${esc(
            formatDuration(FREE_COOLDOWN_MS)
          )}</div></div>
          <div class="tile"><b>TTL</b><div class="sub" style="margin-top:6px">${esc(
            formatDuration(FREE_CLAIM_TTL_MS)
          )}</div></div>
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
          title: "Free key dang khoi dong",
          heading: "Free key dang khoi dong",
          alertType: "err",
          alert: "He thong free dang khoi dong, thu lai sau it giay."
        })
      );
    }
    next();
  });

  app.get(`/${FREE_ROUTE}`, (req, res) => {
    res.send(renderPage());
  });

  app.post("/api/free/start", async (req, res) => {
    const browserId = String(req.body.browserId || "").trim();
    if (!browserId || browserId.length < 8) {
      return res.status(400).json({ ok: false, msg: "Browser ID khong hop le." });
    }

    cleanupFreeStore();

    const { claim, identityHash, browserHash } = createClaim(browserId, req);
    const cooldownUntil = Number(freeStore.cooldowns[identityHash] || 0);
    if (cooldownUntil > Date.now()) {
      return res.status(429).json({
        ok: false,
        msg: `Ban vua nhan key free. Thu lai sau ${formatDuration(
          cooldownUntil - Date.now()
        )}.`
      });
    }

    freeStore.claims[claim.id] = claim;
    await saveStore();

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
      return res.status(500).json({
        ok: false,
        msg: err.message || "Khong tao duoc link Link4m."
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

    const state = verifyToken(String(req.query.state || "").trim());
    if (!state || !state.claimId || !state.browserHash || !state.expiresAt) {
      return res.status(400).send(
        renderPage({
          title: "Phien free khong hop le",
          heading: "Phien free khong hop le",
          alertType: "err",
          alert: "Link callback khong hop le hoac da bi sua."
        })
      );
    }

    const cookiePayload = verifyToken(parseCookies(req).ath_free_claim || "");
    if (
      !cookiePayload ||
      cookiePayload.claimId !== state.claimId ||
      cookiePayload.browserHash !== state.browserHash
    ) {
      return res.status(403).send(
        renderPage({
          title: "Khong xac minh duoc phien",
          heading: "Khong xac minh duoc phien",
          alertType: "err",
          alert: "Phien free khong dung trinh duyet da bat dau."
        })
      );
    }

    const claim = freeStore.claims[state.claimId];
    if (!claim) {
      return res.status(404).send(
        renderPage({
          title: "Phien da het han",
          heading: "Phien da het han",
          alertType: "err",
          alert: "Phien vuot link nay da het han hoac da bi xoa."
        })
      );
    }

    const now = Date.now();
    if (claim.expiresAt < now || Number(state.expiresAt) < now) {
      return res.status(410).send(
        renderPage({
          title: "Phien da het han",
          heading: "Phien da het han",
          alertType: "err",
          alert: "Ban can bat dau lai mot phien free moi."
        })
      );
    }

    if (claim.browserHash !== state.browserHash) {
      return res.status(403).send(
        renderPage({
          title: "Sai trinh duyet",
          heading: "Sai trinh duyet",
          alertType: "err",
          alert: "Phien nay khong duoc mo bang trinh duyet da bat dau."
        })
      );
    }

    if (claim.uaHash !== hashValue(getUserAgent(req))) {
      return res.status(403).send(
        renderPage({
          title: "Sai User-Agent",
          heading: "Sai User-Agent",
          alertType: "err",
          alert: "Phien free bi thay doi thiet bi hoac trinh duyet."
        })
      );
    }

    if (now - Number(claim.createdAt || 0) < FREE_MIN_ELAPSED_MS) {
      return res.status(403).send(
        renderPage({
          title: "Hoan tat qua nhanh",
          heading: "Hoan tat qua nhanh",
          alertType: "err",
          alert: "Phien nay quay ve qua nhanh, vui long vuot Link4m dung cach."
        })
      );
    }

    const referer = String(req.headers.referer || "").toLowerCase();
    if (
      FREE_REFERER_KEYWORD &&
      referer &&
      !referer.includes(FREE_REFERER_KEYWORD)
    ) {
      return res.status(403).send(
        renderPage({
          title: "Referer khong hop le",
          heading: "Referer khong hop le",
          alertType: "err",
          alert: "Callback khong den tu Link4m."
        })
      );
    }

    claim.status = "verified";
    claim.verifiedAt = now;

    const issued = await issueKeyForClaim(claim);
    return res.send(
      renderPage({
        title: "Nhan key thanh cong",
        heading: "Nhan key thanh cong",
        sub: "Key duoc tao tu dong sau khi callback hop le.",
        alertType: "ok",
        alert: "Key nay da duoc ghi vao he thong key hien tai.",
        key: issued.key,
        expireText: deps.formatVNTime(issued.expireAt)
      })
    );
  });

  initStore().catch((err) => {
    console.error("Init free key routes failed:", err.message);
  });
};
