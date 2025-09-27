analytical_ai/
├── data/
│ ├── loader.js # ★loadAndProcessRaceData はここ
│ └── preprocessor.js # データの前処理や特徴量エンジニアリングを行う関数
│
├── models/
│ ├── train.js # AI モデルを学習させるロジック
│ └── predict.js # 学習済みモデルを使って予測を行う関数
│
├── analysis/
│ └── backtest.js # 期待値計算や投資シミュレーションを行うロジック
│
├── config/
│ └── index.js # 設定ファイル (データパス、モデルのパラメータなど)
│
└── index.js # ★ 全体の処理を呼び出すメインファイル

source venv/bin/activate
