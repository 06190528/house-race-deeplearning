// 各モジュールと設定をインポート
const { loadAndProcessRaceData } = require("./data/loader.js");
const { preprocessData } = require("./data/preprocessor.js");
const config = require("./config/index.js");

// --- ヘルパー関数群 ---
// 配列の平均値を計算
const average = (arr) => arr.reduce((p, c) => p + c, 0) / arr.length;

// 配列の標準偏差を計算
const stdDev = (arr) => {
  const avg = average(arr);
  const squareDiffs = arr.map((value) => (value - avg) ** 2);
  return Math.sqrt(average(squareDiffs));
};

/**
 * 指定されたパラメータでバックテストを実行する関数
 * @param {Object[]} allProcessedData - 前処理済みの全レースデータ
 * @param {Object} params - { weightA, weightB, thresholdC }
 * @returns {Object} - { roi, investment, return }
 */
const runBacktest = (allProcessedData, params) => {
  let totalInvestment = 0;
  let totalReturn = 0;

  // データをレースごとにグループ化
  const races = allProcessedData.reduce((acc, horse) => {
    acc[horse.raceId] = acc[horse.raceId] || [];
    acc[horse.raceId].push(horse);
    return acc;
  }, {});

  // 各レースでシミュレーション
  for (const raceId in races) {
    const horsesInRace = races[raceId];

    // 1. 各馬の「うまみスコア」を計算
    const scores = horsesInRace.map(
      (h) => params.weightA / h.popularity + params.weightB * h.winOdds
    );

    // 2. レースのスコア平均値と標準偏差を計算
    const raceScoreAvg = average(scores);
    const raceScoreStdDev = stdDev(scores);
    const buyThreshold = raceScoreAvg + raceScoreStdDev * params.thresholdC;

    // 3. 賭ける馬を決定
    horsesInRace.forEach((horse, index) => {
      if (scores[index] > buyThreshold) {
        // 賭ける！ (100円賭けたと仮定)
        totalInvestment += 100;
        if (horse.isWinner === 1) {
          totalReturn += 100 * horse.winOdds;
        }
      }
    });
  }

  if (totalInvestment === 0) return { roi: 0, investment: 0, return: 0 };

  return {
    roi: (totalReturn / totalInvestment) * 100,
    investment: totalInvestment,
    return: totalReturn,
  };
};

// --- メイン実行関数 ---
const main = async () => {
  console.log("1. Loading data...");
  const rawData = await loadAndProcessRaceData(config.dataPath);

  console.log("2. Preprocessing data...");
  const processedData = preprocessData(rawData);
  console.log(`> Found ${processedData.length} valid horse data entries.`);

  console.log("3. Starting backtest to find the best parameters...");

  let bestResult = { roi: 0 };
  let bestParams = {};

  // パラメータA, B, Cをループさせて最適な組み合わせを探す
  for (let weightA = 1; weightA <= 100; weightA += 5) {
    for (let weightB = 1; weightB <= 100; weightB += 5) {
      for (let thresholdC = 0.8; thresholdC <= 2.0; thresholdC += 0.2) {
        const params = { weightA, weightB, thresholdC: thresholdC.toFixed(1) };
        const result = runBacktest(processedData, params);

        if (result.roi > bestResult.roi) {
          bestResult = result;
          bestParams = params;
          console.log(
            `✨ New Best ROI: ${result.roi.toFixed(2)}% | ` +
              `Params (A:${params.weightA}, B:${params.weightB}, C:${params.thresholdC})`
          );
        }
      }
    }
  }

  console.log("\n--- 🚀 All tasks completed! ---");
  if (bestResult.roi > 0) {
    console.log("Best Parameters Found:");
    console.log(bestParams);
    console.log("Best Backtest Result:");
    console.log({
      ...bestResult,
      roi: `${bestResult.roi.toFixed(2)}%`,
    });
  } else {
    console.log(
      "No profitable strategy was found with the given parameter ranges."
    );
  }
};

// 実行
main();
