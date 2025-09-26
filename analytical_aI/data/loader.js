const fs = require("fs").promises;
const path = require("path");

/**
 * 指定されたディレクトリから全てのレースデータを読み込み、
 * raceIdを各レコードに追加して単一の配列に変換する関数。
 *
 * @param {string} dataPath - 現在のファイルから見た、データディレクトリへの相対パス
 * @returns {Promise<Object[]>} 全ての馬の成績データを含む単一の配列
 */
const loadAndProcessRaceData = async (dataPath) => {
  const dataDir = dataPath;
  console.log(`📂 Reading data from: ${dataDir}`);

  const allProcessedData = [];

  try {
    const files = await fs.readdir(dataDir);

    for (const file of files) {
      if (path.extname(file) === ".json") {
        const filePath = path.join(dataDir, file);

        const fileContent = await fs.readFile(filePath, "utf-8");
        const singleRaceData = JSON.parse(fileContent); // 1レース分の馬データ配列

        const raceId = path.basename(file, ".json"); // ファイル名から拡張子を除去してID取得
        const processedRaceData = singleRaceData.map((horseResult) => ({
          raceId: raceId,
          ...horseResult,
        }));

        // 6. 全データ格納用の配列に結果を追加
        allProcessedData.push(...processedRaceData);
      }
    }
  } catch (error) {
    console.error(`[Error] Failed to read or process data:`, error);
  }

  console.log(
    `✅ Successfully loaded and processed data for ${allProcessedData.length} horses.`
  );
  return allProcessedData;
};

module.exports = { loadAndProcessRaceData };
