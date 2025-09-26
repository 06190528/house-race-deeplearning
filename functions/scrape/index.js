// 必要なライブラリをインポート
const fs = require("fs").promises; // ファイル操作用
const path = require("path"); // パス操作用
const axios = require("axios");
const cheerio = require("cheerio");
const iconv = require("iconv-lite");

// --- ユーティリティ関数: 指定ミリ秒だけ処理を待機 ---
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// --- (前回作成) 日付からレースIDリストを取得 ---
const fetchRaceIdsByDate = async (dateString) => {
  const url = `https://db.netkeiba.com/race/list/${dateString}/`;
  try {
    const response = await axios.get(url, { responseType: "arraybuffer" });
    const html = iconv.decode(Buffer.from(response.data), "EUC-JP");
    const $ = cheerio.load(html);
    const raceIds = [];
    $('a[href^="/race/"]').each((i, el) => {
      const href = $(el).attr("href");
      const match = href.match(/\/race\/(\d{12})\//);
      if (match && match[1] && !raceIds.includes(match[1])) {
        raceIds.push(match[1]);
      }
    });
    return raceIds;
  } catch (error) {
    // 404エラーなどは開催日ではないため、エラーログは出さずに空配列を返す
    if (error.response && error.response.status === 404) {
      return [];
    }
    console.error(
      `[Error] Failed to fetch race IDs for ${dateString}: ${error.message}`
    );
    return [];
  }
};

// --- (前回作成) レースIDから詳細データを取得 ---
const fetchRaceData = async (raceId) => {
  const url = `https://db.netkeiba.com/race/${raceId}/`;
  try {
    const response = await axios.get(url, { responseType: "arraybuffer" });
    const html = iconv.decode(Buffer.from(response.data), "EUC-JP");
    const $ = cheerio.load(html);
    const raceResults = [];
    $(".race_table_01 tbody tr").each((i, row) => {
      const cells = $(row).find("td");
      if (cells.length === 0) return;
      raceResults.push({
        rank: cells.eq(0).text().trim(),
        frameNumber: cells.eq(1).text().trim(),
        horseNumber: cells.eq(2).text().trim(),
        horseName: cells.eq(3).text().trim(),
        sexAndAge: cells.eq(4).text().trim(),
        weightCarried: cells.eq(5).text().trim(),
        jockey: cells.eq(6).text().trim(),
        time: cells.eq(7).text().trim(),
        margin: cells.eq(8).text().trim(),
        passingOrder: cells.eq(10).text().trim(),
        last3Furlongs: cells.eq(11).text().trim(),
        winOdds: cells.eq(12).text().trim(),
        popularity: cells.eq(13).text().trim(),
        horseWeight: cells.eq(14).text().trim(),
        trainer: cells.eq(18).find("a").text().trim(),
        owner: cells.eq(19).find("a").text().trim(),
        prizeMoney: cells.eq(20).text().trim(),
      });
    });
    return raceResults;
  } catch (error) {
    console.error(
      `[Error] Failed to fetch race data for ${raceId}: ${error.message}`
    );
    return [];
  }
};

/**
 * 過去N日間のレースデータを取得し、JSONファイルとして保存するメイン関数
 * @param {number} days - 取得する過去の日数
 * @param {number} delay - 各レース取得間の待機時間 (ミリ秒)
 */
const scrapeAndSavePastRaces = async (days, delay) => {
  const outputDir = path.join(__dirname, "racedata");
  await fs.mkdir(outputDir, { recursive: true });

  // 1. 最初にracedataフォルダを読み込み、既存のレースIDをセットに格納
  const existingRaceIds = new Set();
  try {
    const files = await fs.readdir(outputDir);
    for (const file of files) {
      if (file.endsWith(".json")) {
        // ".json"の部分を取り除いてIDとしてセットに追加
        existingRaceIds.add(path.basename(file, ".json"));
      }
    }
    console.log(
      `📂 Found ${existingRaceIds.size} existing race data files. These will be skipped.`
    );
  } catch (error) {
    console.error("Could not read racedata directory.", error);
  }

  console.log(`🚀 Starting scrape for the past ${days} days...`);

  for (let i = 0; i < days; i++) {
    const targetDate = new Date();
    targetDate.setDate(targetDate.getDate() - i);

    const year = targetDate.getFullYear();
    const month = String(targetDate.getMonth() + 1).padStart(2, "0");
    const day = String(targetDate.getDate()).padStart(2, "0");
    const dateString = `${year}${month}${day}`;

    console.log(`\n--- Processing Date: ${dateString} ---`);

    const raceIds = await fetchRaceIdsByDate(dateString);
    if (raceIds.length === 0) {
      console.log(`No races found for ${dateString}. Skipping.`);
      continue;
    }

    console.log(`Found ${raceIds.length} races. Checking for new data...`);

    for (const raceId of raceIds) {
      // 2. 既存IDのセットに現在のraceIdが含まれているかチェック
      if (existingRaceIds.has(raceId)) {
        // 含まれていればスキップ
        console.log(`⏩ Skipping race ${raceId}: File already exists.`);
        continue;
      }

      // 含まれていない場合のみ、データを取得して保存する
      console.log(`Fetching data for race: ${raceId}`);
      const raceData = await fetchRaceData(raceId);

      if (raceData && raceData.length > 0) {
        const filePath = path.join(outputDir, `${raceId}.json`);
        await fs.writeFile(filePath, JSON.stringify(raceData, null, 2));
        console.log(`✅ Saved data to ${filePath}`);
      } else {
        console.log(`⚠️ No data found for race ${raceId}. Skipping save.`);
      }

      console.log(`Waiting for ${delay / 1000} seconds...`);
      await sleep(delay);
    }
  }

  console.log("\n🎉 All tasks completed!");
};

// --- メイン処理の実行 ---
// 過去30日分のデータを、1.5秒間隔で取得する
scrapeAndSavePastRaces(80, 500);
