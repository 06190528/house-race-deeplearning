import sys
import os
import numpy as np
import pandas as pd
from joblib import load

# --- プロジェクトルートをPythonの検索パスに追加 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# --- モジュールをインポート ---
from analytical_aI.config.index import MODELS_DIR, UNTOUCHED_DATA_DIR
from analytical_aI.data.loader import load_and_preprocess_data
from analytical_aI.data.preprocessor import FEATURE_COLS

def main(bet_threshold: float = 1.2, win_rate_threshold: float = 0.05, race_budget: int = 100):
    """
    未知データでバックテストを実行する。
    LambdaRankの予測スコアをSoftmaxで勝率に変換し、期待値(EV)を計算する。
    """
    print("--- 未知データによるバックテストを開始します ---")

    # --- 1. モデルとデータの読み込み ---
    model_path = MODELS_DIR / 'lambdarank_model.joblib'
    print(f"1. 学習済みモデル '{model_path.name}' を読み込みます...")
    try:
        model = load(model_path)
    except FileNotFoundError:
        print(f"エラー: モデルファイル '{model_path}' が見つかりません。先に train.py を実行してください。")
        return

    print(f"2. 未知データを読み込み、前処理します...")
    df_untouched, _ = load_and_preprocess_data(UNTOUCHED_DATA_DIR)
    if df_untouched.empty:
        print("エラー: 未知データが読み込めませんでした。")
        return

    # --- 2. スコア予測とSoftmaxによる勝率（確率）化 ---
    print("3. 相対スコアを予測し、Softmaxで勝率に変換、期待値を計算します...")

    available_features = [f for f in FEATURE_COLS if f in df_untouched.columns]
    X_untouched = df_untouched[available_features]

    df_untouched['predicted_score'] = model.predict(X_untouched)

    def softmax(x):
        e_x = np.exp(x - np.max(x))
        return e_x / e_x.sum()

    df_untouched['predicted_win_rate'] = df_untouched.groupby('race_id')['predicted_score'].transform(
        lambda x: softmax(x.values)
    )

    # 期待値(EV) = 予測勝率 × 単勝オッズ
    # ※ 新JSONフォーマットではフィールド名は 'odds'（旧 'winOdds' から変更）
    df_untouched['expected_value'] = df_untouched['predicted_win_rate'] * df_untouched['odds']

    # --- 3. 期待値に基づくシミュレーション ---
    print(f"4. 期待値が {bet_threshold} を超える馬に、勝率に応じて資金を配分する戦略でシミュレーションします...")

    total_investment = 0
    total_return = 0
    bet_races = 0
    num_bets = 0

    for race_id, race_df in df_untouched.groupby('race_id'):
        value_bets = value_bets = race_df[
             (race_df['expected_value'] > bet_threshold) &
            (race_df['predicted_win_rate'] > win_rate_threshold)
        ]

        if value_bets.empty:
            continue

        bet_races += 1
        num_bets += len(value_bets)
        total_investment += race_budget

        value_prob_sum = value_bets['predicted_win_rate'].sum()

        for index, horse in value_bets.iterrows():
            bet_amount = race_budget * (horse['predicted_win_rate'] / value_prob_sum)
            if horse['label'] == 3:  # label=3 が1着
                total_return += bet_amount * horse['odds']

    # --- 4. 結果の評価 ---
    roi = (total_return / total_investment) * 100 if total_investment > 0 else 0
    total_races = df_untouched['race_id'].nunique()

    print("\n--- バックテスト結果 ---")
    print(f"対象レース数: {total_races} レース")
    print(f"賭け対象レース数: {bet_races} レース ({ (bet_races/total_races)*100:.2f} %)")
    print(f"賭け対象の馬の総数: {num_bets} 頭")
    print(f"総投資額: {total_investment:,.0f} 円")
    print(f"総払戻額: {total_return:,.0f} 円")
    print("-------------------------")
    print(f"回収率 (ROI): {roi:.2f} %")
    print("-------------------------")

if __name__ == "__main__":
    main(bet_threshold=1.2, win_rate_threshold=0.15)
