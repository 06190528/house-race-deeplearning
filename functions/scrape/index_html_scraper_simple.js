const fs = require("fs").promises;
const path = require("path");
const axios = require("axios");
const cheerio = require("cheerio");
const iconv = require("iconv-lite");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const fetchRaceIdsByDate = async (dateString) => {
  const url = `https://db.netkeiba.com/race/list/${dateString}/`;
  try {
    const response = await axios.get(url, { responseType: "arraybuffer" });
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
const fetchRaceHtml = async (raceId) => {
  const url = `https://db.netkeiba.com/race/${raceId}/`;
  try {
    const response = await axios.get(url, { responseType: "arraybuffer" });
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

  const oldestCachedDate = cachedDates.size > 0 ? [...cachedDates].sort()[0] : null;
  if (oldestCachedDate) {
    console.log(`📅 最古キャッシュ日: ${oldestCachedDate} — それより新しい日付はスキップ`);
  }

  console.log(`🚀 過去 ${days} 日分のスクレイピング開始...\n`);

  for (let i = 0; i < days; i++) {
    const targetDate = new Date();
    targetDate.setDate(targetDate.getDate() - i);
    const year = targetDate.getFullYear();
    const month = String(targetDate.getMonth() + 1).padStart(2, "0");
    const day = String(targetDate.getDate()).padStart(2, "0");
    const dateString = `${year}${month}${day}`;

    if (oldestCachedDate && dateString > oldestCachedDate) {
      console.log(`⏩ ${dateString}: キャッシュ済みのためスキップ`);
      continue;
    }

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

      console.log(`  取得中: ${raceId}`);
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
