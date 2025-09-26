/**
 * 生のレースデータを分析可能な形式に前処理する関数
 * @param {Object[]} rawData - loader.jsで読み込んだ生のデータ配列
 * @returns {Object[]} 前処理済みのデータ配列
 */
const preprocessData = (rawData) => {
  const processed = rawData
    .map((horse) => {
      const rank = parseInt(horse.rank, 10);
      const popularity = parseInt(horse.popularity, 10);
      const winOdds = parseFloat(horse.winOdds);

      // データが数値でない、またはオッズが0の場合は分析対象外とする
      if (isNaN(rank) || isNaN(popularity) || isNaN(winOdds) || winOdds === 0) {
        return null;
      }

      return {
        raceId: horse.raceId,
        horseNumber: parseInt(horse.horseNumber, 10),
        horseName: horse.horseName,
        rank: rank,
        popularity: popularity,
        winOdds: winOdds,
        isWinner: rank === 1 ? 1 : 0, // 1着なら1, それ以外は0
      };
    })
    .filter(Boolean); // nullになった要素を取り除く

  return processed;
};

// 関数をエクスポート
module.exports = {
  preprocessData,
};
