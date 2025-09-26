const path = require("path");

// 設定をエクスポート
module.exports = {
  // analytical_ai/ から見たracedataフォルダへの相対パス
  dataPath: path.join(__dirname, "../../functions/scrape/racedata"),
};
