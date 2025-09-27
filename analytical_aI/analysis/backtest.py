import sys
import os
import pandas as pd
from joblib import load

# --- プロジェクトルートをPythonの検索パスに追加 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# --- モジュールをインポート ---
from analytical_aI.config.index import MODELS_DIR, UNTOUCHED_DATA_DIR
from analytical_aI.data.loader import load_and_preprocess_data

def main(bet_threshold: float = 1.2, race_budget: int = 100):
    """
    未知データでバックテストを実行する。
    期待値でフィルタリングし、確率に応じて資金を比例配分する戦略を用いる。
    """
    print("--- 未知データによるバックテストを開始します ---")

    # --- 1. モデルとデータの読み込み ---
    model_path = MODELS_DIR / 'lightgbm_model.joblib'
    print(f"1. 学習済みモデル '{model_path.name}' を読み込みます...")
    try:
        model = load(model_path)
    except FileNotFoundError:
        # ... (エラー処理は変更なし) ...
        return

    print(f"2. 未知データを読み込み、前処理します...")
    df_untouched = load_and_preprocess_data(UNTOUCHED_DATA_DIR)
    if df_untouched.empty:
        # ... (エラー処理は変更なし) ...
        return

    # --- 2. 勝率予測と期待値計算 ---
    print("3. 勝率を予測し、期待値を計算します...")
    features = [
        'popularity', 'jockeyWinRate', 'age', 'weightCarried',
        'horseWeight_val', 'horseWeight_change', 'winOdds'
    ]
    features = [f for f in features if f in df_untouched.columns]
    X_untouched = df_untouched[features].fillna(df_untouched[features].mean())

    df_untouched['predicted_win_rate'] = model.predict_proba(X_untouched)[:, 1]
    race_prob_sum = df_untouched.groupby('raceId')['predicted_win_rate'].transform('sum')
    df_untouched['normalized_win_rate'] = df_untouched['predicted_win_rate'] / race_prob_sum
    df_untouched['expected_value'] = df_untouched['normalized_win_rate'] * df_untouched['winOdds']

    # --- 3. 新しい賭け方モデルによるシミュレーション ---
    print(f"4. 期待値が{bet_threshold}を超える馬に、勝率に応じて資金を配分する戦略でシミュレーションします...")

    total_investment = 0
    total_return = 0
    bet_races = 0
    num_bets = 0

    # レースごとに処理
    for race_id, race_df in df_untouched.groupby('raceId'):
        # 期待値が閾値を超える馬だけをフィルタリング
        value_bets = race_df[race_df['expected_value'] > bet_threshold]

        # 買うべき馬がいなければ、このレースは見送り
        if value_bets.empty:
            continue
        
        bet_races += 1
        num_bets += len(value_bets)
        total_investment += race_budget

        # 買うべき馬たちの正規化勝率の合計を計算
        value_prob_sum = value_bets['normalized_win_rate'].sum()

        # 資金を比例配分して、払い戻しを計算
        for index, horse in value_bets.iterrows():
            bet_amount = race_budget * (horse['normalized_win_rate'] / value_prob_sum)
            if horse['isWinner'] == 1:
                total_return += bet_amount * horse['winOdds']
    
    # --- 4. 結果の評価 ---
    roi = (total_return / total_investment) * 100 if total_investment > 0 else 0
    total_races = df_untouched['raceId'].nunique()
    
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
        main(bet_threshold=1.2) # 閾値を少し厳しくして試す