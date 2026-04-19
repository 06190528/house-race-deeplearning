const fs = require("fs").promises;
const path = require("path");
const axios = require("axios");
const cheerio = require("cheerio");
const iconv = require("iconv-lite");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ---------------------------------------------------------------------------
// プロキシキャッシュ
// ---------------------------------------------------------------------------

const PROXY_CACHE_FILE = path.join(__dirname, "proxy_cache.json");
const WORKING_TTL_MS = 30 * 60 * 1000;
const FAILED_TTL_MS = 6 * 60 * 60 * 1000;

const loadProxyCache = async () => {
  try {
    const data = await fs.readFile(PROXY_CACHE_FILE, "utf-8");
    return JSON.parse(data);
  } catch {
    return { working: [], failed: [] };
  }
};

const saveProxyCache = async (working, failed) => {
  const data = {
    updatedAt: new Date().toISOString(),
    working,
    failed,
  };
  await fs.writeFile(PROXY_CACHE_FILE, JSON.stringify(data, null, 2));
  console.log(`💾 プロキシキャッシュ保存: 動作中 ${working.length} 件 / 失敗 ${failed.length} 件`);
};

// ---------------------------------------------------------------------------
// プロキシ管理
// ---------------------------------------------------------------------------

const fetchProxyList = async () => {
  const sources = [
    "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=5000&country=all",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt",
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps",
    "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt"
  ];

  const proxies = [];
  for (const url of sources) {
    try {
      const res = await axios.get(url, { timeout: 10000 });
      const lines = String(res.data).split("\n");
      for (const line of lines) {
        const match = line.trim().match(/^(\d+\.\d+\.\d+\.\d+):(\d+)$/);
        if (match) {
          proxies.push({ host: match[1], port: parseInt(match[2]), lastUsed: 0 });
        }
      }
      console.log(`  ✅ ${url.split("?")[0]} から ${proxies.length} 件取得`);
    } catch {
      console.log(`  ⚠️ ${url.split("?")[0]} の取得失敗`);
    }
  }
  const seen = new Set();
  return proxies.filter(({ host, port }) => {
    const key = `${host}:${port}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

const PROXY_TIMEOUT = 6000;
const testProxy = async (proxy) => {
  try {
    await axios.get("https://db.netkeiba.com/", {
      proxy: { host: proxy.host, port: proxy.port },
      timeout: PROXY_TIMEOUT,
      responseType: "arraybuffer",
    });
    return true;
  } catch {
    return false;
  }
};

const buildWorkingProxies = async (candidates, concurrency = 30) => {
  const cache = await loadProxyCache();
  const now = Date.now();

  const workingMap = new Map(cache.working.map((p) => [`${p.host}:${p.port}`, p]));
  const failedMap = new Map(cache.failed.map((p) => [`${p.host}:${p.port}`, p]));

  const freshWorking = [];
  const toTest = [];
  const failedCache = [];

  for (const candidate of candidates) {
    const key = `${candidate.host}:${candidate.port}`;

    if (workingMap.has(key)) {
      const cached = workingMap.get(key);
      if (now - cached.lastChecked < WORKING_TTL_MS) {
        freshWorking.push({ ...candidate, lastChecked: cached.lastChecked });
        continue;
      }
    }

    if (failedMap.has(key)) {
      const cached = failedMap.get(key);
      if (now - cached.lastChecked < FAILED_TTL_MS) {
        failedCache.push(cached);
        continue;
      }
    }

    toTest.push(candidate);
  }

  console.log(`🔍 キャッシュ: 動作中 ${freshWorking.length} 件を即採用 / ${toTest.length} 件を新規テスト（${failedCache.length} 件は失敗キャッシュでスキップ）`);

  const newWorking = [];
  const newFailed = [];

  for (let i = 0; i < toTest.length; i += concurrency) {
    const batch = toTest.slice(i, i + concurrency);
    const results = await Promise.all(
      batch.map(async (proxy) => ({ proxy, ok: await testProxy(proxy) }))
    );
    for (const { proxy, ok } of results) {
      if (ok) {
        newWorking.push({ ...proxy, lastChecked: Date.now() });
      } else {
        newFailed.push({ host: proxy.host, port: proxy.port, lastChecked: Date.now() });
      }
    }
    process.stdout.write(
      `\r  テスト済み: ${Math.min(i + concurrency, toTest.length)} / ${toTest.length}  新規OK: ${newWorking.length}`
    );
  }
  if (toTest.length > 0) console.log();

  const allWorking = [...freshWorking, ...newWorking];
  const allFailed = [...failedCache, ...newFailed];
  await saveProxyCache(allWorking, allFailed);

  return allWorking;
};

// ---------------------------------------------------------------------------
// リクエスト（プロキシローテーション付き）
// ---------------------------------------------------------------------------

const PROXY_REST_MS = 2000;
const GIVE_UP_MS = 60000;

const axiosWithProxy = async (url, proxyPool) => {
  const start = Date.now();

  while (Date.now() - start < GIVE_UP_MS) {
    if (proxyPool.length === 0) throw new Error(`${url}: プロキシプールが空です`);

    const now = Date.now();
    const available = proxyPool.filter((p) => now - p.lastUsed >= PROXY_REST_MS);

    if (available.length === 0) {
      const nextMs = Math.min(...proxyPool.map((p) => p.lastUsed + PROXY_REST_MS));
      await sleep(Math.max(10, nextMs - Date.now()));
      continue;
    }

    const proxy = available[Math.floor(Math.random() * available.length)];
    proxy.lastUsed = Date.now();

    try {
      const response = await axios.get(url, {
        proxy: { host: proxy.host, port: proxy.port },
        timeout: PROXY_TIMEOUT,
        responseType: "arraybuffer",
      });
      return response;
    } catch {
      const idx = proxyPool.indexOf(proxy);
      if (idx !== -1) proxyPool.splice(idx, 1);
    }
  }

  throw new Error(`${url}: ${GIVE_UP_MS / 1000}秒以内に取得できませんでした`);
};

// ---------------------------------------------------------------------------
// スクレイピング関数
// ---------------------------------------------------------------------------

const fetchRaceIdsByDate = async (dateString, proxyPool) => {
  const url = `https://db.netkeiba.com/race/list/${dateString}/`;
  try {
    const response = await axiosWithProxy(url, proxyPool);
    const html = iconv.decode(Buffer.from(response.data), "EUC-JP");
    const $ = cheerio.load(html);
    const raceIds = [];
    $('a[href^="/race/"]').each((_i, el) => {
      const href = $(el).attr("href");
      const match = href.match(/\/race\/(\d{12})\//);
      if (match && match[1] && !raceIds.includes(match[1])) {
        raceIds.push(match[1]);
      }
    });
    return raceIds;
  } catch (error) {
    if (error.response && error.response.status === 404) return [];
    console.error(`[Error] fetchRaceIdsByDate(${dateString}): ${error.message}`);
    return [];
  }
};

// レースページを取得し、広告・ナビ等を除去したクリーン HTML を返す
const fetchRaceHtml = async (raceId, proxyPool) => {
  const url = `https://db.netkeiba.com/race/${raceId}/`;
  try {
    const response = await axiosWithProxy(url, proxyPool);
    const html = iconv.decode(Buffer.from(response.data), "EUC-JP");
    const $ = cheerio.load(html);

    // レースデータが存在しない場合は null を返す
    if ($(".race_table_01").length === 0) return null;

    // 広告・ナビ・スクリプト等の不要要素を削除
    $("script, style, link, iframe, noscript").remove();
    $("header, footer, nav").remove();
    $(".genre_menu, .race_deta_menu, .race_num").remove();
    $('[id^="div-gpt-ad-"]').remove();
    $(".gam_ad, .gam_ad_raceside01").remove();
    $(".diary_snap_write_box, .master_course_infoText01, .master_course_infoText02").remove();
    $(".LapTimeArea, #Race_Note_Form, #side").remove();

    return $.html();
  } catch (error) {
    console.error(`[Error] fetchRaceHtml(${raceId}): ${error.message}`);
    return null;
  }
};

// ---------------------------------------------------------------------------
// メイン
// ---------------------------------------------------------------------------

const DATE_CONCURRENCY = 5;
const RACE_CONCURRENCY = 10;

const processDate = async (dateString, existingRaceIds, proxyPool, outputDir) => {
  const raceIds = await fetchRaceIdsByDate(dateString, proxyPool);
  if (raceIds.length === 0) {
    console.log(`[${dateString}] 開催なし`);
    return;
  }

  const newRaceIds = raceIds.filter((id) => !existingRaceIds.has(id));
  if (newRaceIds.length === 0) {
    console.log(`[${dateString}] ⏩ 全 ${raceIds.length} レースキャッシュ済み`);
    return;
  }

  console.log(`[${dateString}] ${raceIds.length} レース中 ${newRaceIds.length} 件を並列取得...`);

  for (let i = 0; i < newRaceIds.length; i += RACE_CONCURRENCY) {
    const batch = newRaceIds.slice(i, i + RACE_CONCURRENCY);
    await Promise.all(
      batch.map(async (raceId) => {
        const filePath = path.join(outputDir, `${raceId}.html`);
        try {
          await fs.access(filePath);
          existingRaceIds.add(raceId);
          console.log(`  ⏩ [${dateString}] ${raceId}: 他プロセスが取得済み`);
          return;
        } catch { /* ファイルなし → 取得する */ }

        const cleanedHtml = await fetchRaceHtml(raceId, proxyPool);
        if (cleanedHtml) {
          await fs.writeFile(filePath, cleanedHtml, "utf-8");
          existingRaceIds.add(raceId);
          console.log(`  ✅ [${dateString}] ${raceId}`);
        } else {
          console.log(`  ⚠️ [${dateString}] データなし: ${raceId}`);
        }
      })
    );
  }
};

const scrapeAndSavePastRaces = async (days) => {
  const outputDir = path.join(__dirname, "raceRawHtmlData");
  await fs.mkdir(outputDir, { recursive: true });

  const existingRaceIds = new Set();
  const cachedDates = new Set();
  try {
    const files = await fs.readdir(outputDir);
    for (const file of files) {
      if (file.endsWith(".html")) {
        const raceId = path.basename(file, ".html");
        existingRaceIds.add(raceId);
        cachedDates.add(raceId.substring(0, 8));
      }
    }
    console.log(`📂 既存: ${existingRaceIds.size} レース / ${cachedDates.size} 日分`);
  } catch (error) {
    console.error("raceRawHtmlData 読み込みエラー:", error);
  }

  const oldestCachedDate = cachedDates.size > 0 ? [...cachedDates].sort()[0] : null;
  if (oldestCachedDate) {
    console.log(`📅 最古キャッシュ日: ${oldestCachedDate} — それより新しい日付はスキップ`);
  }

  console.log("\n🌐 フリープロキシリストを取得中...");
  const candidates = await fetchProxyList();
  console.log(`  合計 ${candidates.length} 件の候補`);

  const proxyPool = await buildWorkingProxies(candidates);
  if (proxyPool.length === 0) {
    console.error("❌ 使えるプロキシが見つかりませんでした。");
    return;
  }
  console.log(`✅ ${proxyPool.length} 件のプロキシ確保 (日付 ${DATE_CONCURRENCY} 並列 × レース ${RACE_CONCURRENCY} 並列)\n`);

  const targetDates = [];
  for (let i = 0; i < days; i++) {
    const d = new Date();
    d.setDate(d.getDate() - i);
    const dateString = `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(d.getDate()).padStart(2, "0")}`;
    if (!oldestCachedDate || dateString <= oldestCachedDate) {
      targetDates.push(dateString);
    }
  }
  console.log(`🚀 処理対象: ${targetDates.length} 日分（${days - targetDates.length} 日はスキップ済み）\n`);

  for (let i = 0; i < targetDates.length; i += DATE_CONCURRENCY) {
    const batch = targetDates.slice(i, i + DATE_CONCURRENCY);

    if (proxyPool.length < DATE_CONCURRENCY * RACE_CONCURRENCY) {
      console.log("⚠️  プロキシ補充中...");
      const fresh = await fetchProxyList();
      const freshWorking = await buildWorkingProxies(fresh);
      proxyPool.push(...freshWorking);
      console.log(`  補充後: ${proxyPool.length} 件`);
    }

    await Promise.all(
      batch.map((dateString) => processDate(dateString, existingRaceIds, proxyPool, outputDir))
    );

    console.log(`進捗: ${Math.min(i + DATE_CONCURRENCY, targetDates.length)} / ${targetDates.length} 日完了`);
  }

  console.log("\n🎉 完了！");
};

scrapeAndSavePastRaces(34000);
