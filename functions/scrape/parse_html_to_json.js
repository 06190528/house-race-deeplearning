const fs = require("fs").promises;
const path = require("path");
const cheerio = require("cheerio");

const RAW_DIR = path.join(__dirname, "raceRawHtmlData");
const OUT_DIR = path.join(__dirname, "raceData");

// ---------------------------------------------------------------------------
// ユーティリティ
// ---------------------------------------------------------------------------

const parseTimeToSeconds = (str) => {
  if (!str || !str.trim()) return null;
  str = str.trim();
  const m1 = str.match(/^(\d+):(\d+\.\d+)$/);
  if (m1) return parseInt(m1[1]) * 60 + parseFloat(m1[2]);
  const m2 = str.match(/^(\d+\.\d+)$/);
  if (m2) return parseFloat(m2[1]);
  return null;
};

// ---------------------------------------------------------------------------
// レース情報
// ---------------------------------------------------------------------------

const parseRaceInfo = ($) => {
  const info = {};

  // コース・天候・馬場状態
  const spanText = $("dl.racedata span").first().text().replace(/\s+/g, " ").trim();

  const courseMatch = spanText.match(/([芝ダ障])(右|左)?(\d+)m/);
  if (courseMatch) {
    info.track_type = courseMatch[1];
    info.direction = courseMatch[2] || null;
    info.distance = parseInt(courseMatch[3]);
  }

  const weatherMatch = spanText.match(/天候\s*:\s*(\S+)/);
  info.weather = weatherMatch ? weatherMatch[1] : null;

  const conditionMatch = spanText.match(/(?:芝|ダート)\s*:\s*(\S+)/);
  info.track_condition = conditionMatch ? conditionMatch[1] : null;

  // ラップタイム・ペース
  info.lap_times = null;
  info.pace = null;

  $('table.result_table_02[summary="ラップタイム"] tr').each((_i, row) => {
    const th = $(row).find("th").text().trim();
    const td = $(row).find("td").text().trim();
    if (th === "ラップ") {
      const laps = td.split("-").map((s) => parseFloat(s.trim())).filter((n) => !isNaN(n));
      info.lap_times = laps.length > 0 ? laps : null;
    } else if (th === "ペース") {
      const m = td.match(/\((\d+\.\d+)-(\d+\.\d+)\)/);
      if (m) info.pace = { first_3f: parseFloat(m[1]), last_3f: parseFloat(m[2]) };
    }
  });

  // コーナー通過順位
  const corners = {};
  $('table.result_table_02[summary="コーナー通過順位"] tr').each((_i, row) => {
    const th = $(row).find("th").text().trim();
    const td = $(row).find("td").text().trim();
    const m = th.match(/^(\d+)コーナー$/);
    if (m) corners[`c${m[1]}`] = td;
  });
  info.corner_positions = Object.keys(corners).length > 0 ? corners : null;

  return info;
};

// ---------------------------------------------------------------------------
// 馬データ
// ---------------------------------------------------------------------------

const parseHorses = ($) => {
  const horses = [];
  const rows = $("table.race_table_01 tr").slice(1); // ヘッダー行をスキップ

  rows.each((_i, row) => {
    const tds = $(row).find("td");
    if (tds.length < 15) return;

    const td = (i) => $(tds.get(i));
    const horse = {};

    // 着順
    const rankText = td(0).text().trim();
    horse.rank = /^\d+$/.test(rankText) ? parseInt(rankText) : rankText;

    // 枠番
    const frameText = td(1).find("span").text().trim() || td(1).text().trim();
    horse.frame_number = /^\d+$/.test(frameText) ? parseInt(frameText) : null;

    // 馬番
    const numText = td(2).text().trim();
    horse.horse_number = /^\d+$/.test(numText) ? parseInt(numText) : null;

    // 馬名・馬ID
    const horseLink = td(3).find("a");
    horse.horse_name = horseLink.text().trim() || td(3).text().trim();
    const horseHref = horseLink.attr("href") || "";
    const horseIdMatch = horseHref.match(/\/horse\/(\w+)\//);
    horse.horse_id = horseIdMatch ? horseIdMatch[1] : null;

    // 性別・年齢
    const sexAge = td(4).text().trim();
    horse.sex = sexAge.length > 0 ? sexAge[0] : null;
    horse.age = sexAge.length > 1 && /^\d+$/.test(sexAge.slice(1)) ? parseInt(sexAge.slice(1)) : null;

    // 斤量
    const wcText = td(5).text().trim();
    horse.weight_carried = wcText ? parseFloat(wcText) : null;

    // 騎手ID
    const jockeyHref = td(6).find("a").attr("href") || "";
    const jockeyMatch = jockeyHref.match(/\/jockey\/result\/recent\/(\w+)\//);
    horse.jockey_id = jockeyMatch ? jockeyMatch[1] : null;

    // タイム（秒換算）
    horse.time = parseTimeToSeconds(td(7).text().trim());

    // 着差
    const marginText = td(8).text().trim();
    horse.margin = marginText || null;

    // col 9-13 はプレミアム項目のためスキップ

    // コーナー通過順位（個別）
    const cornerText = td(14).text().trim();
    if (cornerText) {
      const parts = cornerText.split("-").map((s) => parseInt(s.trim()));
      horse.corner_passing = parts.every((n) => !isNaN(n)) ? parts : cornerText;
    } else {
      horse.corner_passing = null;
    }

    // 上り3F
    const last3fText = (td(15).find("span").text().trim() || td(15).text().trim());
    horse.last_3f = last3fText && !isNaN(parseFloat(last3fText)) ? parseFloat(last3fText) : null;

    // 単勝オッズ
    const oddsText = td(16).text().trim();
    horse.odds = oddsText && !isNaN(parseFloat(oddsText)) ? parseFloat(oddsText) : null;

    // 人気
    const popText = td(17).find("span").text().trim() || td(17).text().trim();
    horse.popularity = /^\d+$/.test(popText) ? parseInt(popText) : null;

    // 馬体重・増減
    const weightText = td(18).text().trim();
    const weightMatch = weightText.match(/^(\d+)\(([+-]?\d+)\)$/);
    if (weightMatch) {
      horse.horse_weight = parseInt(weightMatch[1]);
      horse.weight_change = parseInt(weightMatch[2]);
    } else {
      horse.horse_weight = /^\d+$/.test(weightText) ? parseInt(weightText) : null;
      horse.weight_change = null;
    }

    // col 19-21 はプレミアム項目のためスキップ

    // 調教師ID
    if (tds.length > 22) {
      const trainerHref = td(22).find("a").attr("href") || "";
      const trainerMatch = trainerHref.match(/\/trainer\/result\/recent\/(\w+)\//);
      horse.trainer_id = trainerMatch ? trainerMatch[1] : null;
    }

    // 馬主ID
    if (tds.length > 23) {
      const ownerHref = td(23).find("a").attr("href") || "";
      const ownerMatch = ownerHref.match(/\/owner\/result\/recent\/(\w+)\//);
      horse.owner_id = ownerMatch ? ownerMatch[1] : null;
    }

    // 賞金
    if (tds.length > 24) {
      const prizeText = td(24).text().trim().replace(/,/g, "");
      horse.prize = prizeText && !isNaN(parseFloat(prizeText)) ? parseFloat(prizeText) : null;
    }

    horses.push(horse);
  });

  return horses;
};

// ---------------------------------------------------------------------------
// 払い戻し
// ---------------------------------------------------------------------------

const BET_TEXT_MAP = {
  単勝: "tansho",
  複勝: "fukusho",
  枠連: "wakuren",
  馬連: "umaren",
  ワイド: "wide",
  馬単: "umatan",
  三連複: "sanrenfuku",
  三連単: "sanrentan",
  枠単: "wakutan",
};

const parsePayoff = ($) => {
  const payoff = {};

  $("table.pay_table_01 tr").each((_i, row) => {
    const th = $(row).find("th");
    const tds = $(row).find("td");
    if (!th.length || tds.length < 2) return;

    const thText = th.text().trim();
    const key = BET_TEXT_MAP[thText];
    if (!key) return;

    // 複数行（複勝・ワイドなど）は改行で分割
    const combos = th.nextAll("td").eq(0).html()
      ? $(tds.get(0)).html().split(/<br\s*\/?>/i).map((s) => cheerio.load(s).text().trim()).filter(Boolean)
      : [$(tds.get(0)).text().trim()];

    const returns = th.nextAll("td").eq(1).html()
      ? $(tds.get(1)).html().split(/<br\s*\/?>/i).map((s) => cheerio.load(s).text().replace(/,/g, "").trim()).filter(Boolean)
      : [$(tds.get(1)).text().replace(/,/g, "").trim()];

    payoff[key] = combos.map((combo, i) => ({
      // 馬単・三連単の "→" を "-" に統一
      combination: combo.replace(/→/g, "-").replace(/\s/g, ""),
      return: returns[i] && /^\d+$/.test(returns[i]) ? parseInt(returns[i]) : null,
    }));
  });

  return payoff;
};

// ---------------------------------------------------------------------------
// メイン
// ---------------------------------------------------------------------------

const parseHtmlFile = async (htmlPath) => {
  const raceId = path.basename(htmlPath, ".html");
  const html = await fs.readFile(htmlPath, "utf-8");
  const $ = cheerio.load(html);

  return {
    race_id: raceId,
    race_info: parseRaceInfo($),
    horses: parseHorses($),
    payoff: parsePayoff($),
  };
};

const main = async () => {
  await fs.mkdir(OUT_DIR, { recursive: true });

  const files = (await fs.readdir(RAW_DIR)).filter((f) => f.endsWith(".html")).sort();
  console.log(`📂 ${files.length} ファイルを処理します\n`);

  let ok = 0, skipped = 0, errors = 0;

  for (let i = 0; i < files.length; i++) {
    const file = files[i];
    const outPath = path.join(OUT_DIR, file.replace(".html", ".json"));

    try {
      await fs.access(outPath);
      skipped++;
      continue;
    } catch { /* 未処理 → 続行 */ }

    try {
      const data = await parseHtmlFile(path.join(RAW_DIR, file));
      await fs.writeFile(outPath, JSON.stringify(data, null, 2), "utf-8");
      ok++;
    } catch (err) {
      console.error(`  ❌ [${i + 1}/${files.length}] ${file}: ${err.message}`);
      errors++;
    }
  }

  console.log(`\n🎉 完了: 新規 ${ok} 件 / スキップ ${skipped} 件 / エラー ${errors} 件`);
};

main();
