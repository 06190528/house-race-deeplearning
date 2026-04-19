// 事前準備:
//   brew install tor
//   tor --ControlPort 9051 --CookieAuthentication 0   ← 制御ポートありで起動
//   npm install socks-proxy-agent

const fs = require("fs").promises;
const path = require("path");
const axios = require("axios");
const cheerio = require("cheerio");
const iconv = require("iconv-lite");
const net = require("net");
const { SocksProxyAgent } = require("socks-proxy-agent");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ---------------------------------------------------------------------------
// Tor 設定
// ---------------------------------------------------------------------------

const TOR_SOCKS_PORT = 9050;
const TOR_CONTROL_PORT = 9051;
const ROTATE_EVERY = 100;  // 何リクエストごとにIPを切り替えるか
const TOR_TIMEOUT = 30000; // Torは遅いので長めに設定

let requestCount = 0;

// Tor制御ポートに NEWNYM シグナルを送って新しい回線(=新IP)に切り替える
const rotateTorIP = () =>
  new Promise((resolve) => {
    const socket = net.connect(TOR_CONTROL_PORT, "127.0.0.1", () => {
      socket.write('AUTHENTICATE ""\r\nSIGNAL NEWNYM\r\nQUIT\r\n');
    });
    socket.on("data", () => {});
    socket.on("close", () => {
      console.log("🔄 Tor IP ローテーション — 新しい回線に切り替えました");
      resolve();
    });
    socket.on("error", (err) => {
      console.warn(`⚠️ Tor制御ポート接続失敗 (${err.message}) — そのまま続行`);
      resolve();
    });
  });

// Tor経由でHTTPリクエストを送る。ROTATE_EVERY回ごとにIPを切り替える
const axiosViaTor = async (url) => {
  requestCount++;

  if (requestCount % ROTATE_EVERY === 0) {
    await rotateTorIP();
    await sleep(10000); // 新しい回線が確立するまで10秒待機
  }

  const agent = new SocksProxyAgent(`socks5://127.0.0.1:${TOR_SOCKS_PORT}`);
  return axios.get(url, {
    httpAgent: agent,
    httpsAgent: agent,
    responseType: "arraybuffer",
    timeout: TOR_TIMEOUT,
  });
};

// ---------------------------------------------------------------------------
// スクレイピング関数
// ---------------------------------------------------------------------------

const fetchRaceIdsByDate = async (dateString) => {
  const url = `https://db.netkeiba.com/race/list/${dateString}/`;
  try {
    const response = await axiosViaTor(url);
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

const fetchRaceHtml = async (raceId) => {
  const url = `https://db.netkeiba.com/race/${raceId}/`;
  try {
    const response = await axiosViaTor(url);
    const html = iconv.decode(Buffer.from(response.data), "EUC-JP");
    const $ = cheerio.load(html);

    if ($(".race_table_01").length === 0) return null;

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

const scrapeAndSavePastRaces = async (days, delay) => {
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

  console.log(`🧅 Tor経由スクレイピング開始 (${ROTATE_EVERY}リクエストごとにIPローテーション)\n`);

  for (let i = 0; i < days; i++) {
    const targetDate = new Date();
    targetDate.setDate(targetDate.getDate() - i);
    const year = targetDate.getFullYear();
    const month = String(targetDate.getMonth() + 1).padStart(2, "0");
    const day = String(targetDate.getDate()).padStart(2, "0");
    const dateString = `${year}${month}${day}`;

    console.log(`\n--- ${dateString} ---`);

    const raceIds = await fetchRaceIdsByDate(dateString);
    if (raceIds.length === 0) {
      console.log(`開催なし`);
      continue;
    }

    const newRaceIds = raceIds.filter((id) => !existingRaceIds.has(id));
    console.log(`${raceIds.length} レース中 ${newRaceIds.length} 件が未取得`);

    if (newRaceIds.length === 0) {
      console.log(`⏩ 全件キャッシュ済み`);
      continue;
    }

    await sleep(delay);

    for (const raceId of newRaceIds) {
      const filePath = path.join(outputDir, `${raceId}.html`);
      try {
        await fs.access(filePath);
        existingRaceIds.add(raceId);
        console.log(`  ⏩ ${raceId}: 他プロセスが取得済み`);
        continue;
      } catch { /* ファイルなし → 取得する */ }

      console.log(`  取得中: ${raceId} (累計${requestCount}リクエスト)`);
      const cleanedHtml = await fetchRaceHtml(raceId);

      if (cleanedHtml) {
        await fs.writeFile(filePath, cleanedHtml, "utf-8");
        existingRaceIds.add(raceId);
        console.log(`  ✅ ${raceId}`);
      } else {
        console.log(`  ⚠️ データなし: ${raceId}`);
      }

      await sleep(delay);
    }
  }

  console.log("\n🎉 完了！");
};

scrapeAndSavePastRaces(34000, 1000);
