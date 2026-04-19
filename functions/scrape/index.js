// 必要なライブラリをインポート
const fs = require("fs").promises; // ファイル操作用
const path = require("path"); // パス操作用
const axios = require("axios");
const cheerio = require("cheerio");
const iconv = require("iconv-lite");

// --- ユーティリティ関数: 指定ミリ秒だけ処理を待機 ---
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// --- 日付からレースIDリストを取得 ---
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

// --- レースIDから詳細データを取得 ---
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
        passingOrder: cells.eq(14).text().trim(),
        last3Furlongs: cells.eq(15).text().trim(),
        winOdds: cells.eq(16).text().trim(),
        popularity: cells.eq(17).text().trim(),
        horseWeight: cells.eq(18).text().trim(),
        trainer: cells.eq(22).find("a").text().trim(),
        owner: cells.eq(23).find("a").text().trim(),
        prizeMoney: cells.eq(24).text().trim(),
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
 * @param {number} delay - 各サイトアクセス後の待機時間 (ミリ秒)
 */
const scrapeAndSavePastRaces = async (days, delay) => {
  const outputDir = path.join(__dirname, "racedata");
  await fs.mkdir(outputDir, { recursive: true });

  // 既存レースIDと、レースID先頭8桁(YYYYMMDD)からキャッシュ済み日付を収集
  const existingRaceIds = new Set();
  const cachedDates = new Set();
  try {
    const files = await fs.readdir(outputDir);
    for (const file of files) {
      if (file.endsWith(".json")) {
        const raceId = path.basename(file, ".json");
        existingRaceIds.add(raceId);
        cachedDates.add(raceId.substring(0, 8)); // YYYYMMDD
      }
    }
    console.log(
      `📂 Found ${existingRaceIds.size} existing races across ${cachedDates.size} dates.`
    );
  } catch (error) {
    console.error("Could not read racedata directory.", error);
  }

  // 最古のキャッシュ済み日付を特定
  // それより新しい日付はすべて完全取得済みのためサイトアクセスなしでスキップ
  // 最古日付のみ途中で止まった可能性があるため再チェックする
  const oldestCachedDate = cachedDates.size > 0
    ? [...cachedDates].sort()[0]
    : null;

  if (oldestCachedDate) {
    console.log(
      `📅 Oldest cached date: ${oldestCachedDate} — newer dates will be skipped entirely.`
    );
  }

  console.log(`🚀 Starting scrape for the past ${days} days...`);

  for (let i = 0; i < days; i++) {
    const targetDate = new Date();
    targetDate.setDate(targetDate.getDate() - i);

    const year = targetDate.getFullYear();
    const month = String(targetDate.getMonth() + 1).padStart(2, "0");
    const day = String(targetDate.getDate()).padStart(2, "0");
    const dateString = `${year}${month}${day}`;

    // 最古キャッシュ日付より新しい日付はサイトへのアクセスなしでスキップ
    if (oldestCachedDate && dateString > oldestCachedDate) {
      console.log(`⏩ ${dateString}: already fully cached. Skipping.`);
      continue;
    }

    console.log(`\n--- Processing Date: ${dateString} ---`);

    const raceIds = await fetchRaceIdsByDate(dateString);
    if (raceIds.length === 0) {
      console.log(`No races found for ${dateString}. Skipping.`);
      continue;
    }

    const newRaceIds = raceIds.filter((id) => !existingRaceIds.has(id));
    console.log(
      `Found ${raceIds.length} races (${newRaceIds.length} new, ${raceIds.length - newRaceIds.length} cached).`
    );

    if (newRaceIds.length === 0) {
      console.log(`⏩ All races for ${dateString} already cached. Skipping.`);
      continue;
    }

    // fetchRaceIdsByDate がサイトにアクセスしたので delay
    await sleep(delay);

    for (const raceId of newRaceIds) {
      const filePath = path.join(outputDir, `${raceId}.json`);
      // 別プロセスがすでに保存済みかをファイル存在確認で検出
      try {
        await fs.access(filePath);
        existingRaceIds.add(raceId);
        console.log(`⏩ ${raceId}: 他プロセスが取得済み。スキップ。`);
        continue;
      } catch { /* ファイルなし → 取得する */ }

      console.log(`Fetching data for race: ${raceId}`);
      const raceData = await fetchRaceData(raceId);

      if (raceData && raceData.length > 0) {
        await fs.writeFile(filePath, JSON.stringify(raceData, null, 2));
        existingRaceIds.add(raceId);
        console.log(`✅ Saved data to ${filePath}`);
      } else {
        console.log(`⚠️ No data found for race ${raceId}. Skipping save.`);
      }

      // fetchRaceData がサイトにアクセスしたので delay
      console.log(`Waiting for ${delay / 1000} seconds...`);
      await sleep(delay);
    }
  }

  console.log("\n🎉 All tasks completed!");
};

// --- メイン処理の実行 ---
scrapeAndSavePastRaces(34000, 1500);
