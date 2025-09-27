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

def main(bet_threshold: float = 1.0):
    """
    未知データと学習済みモデルを使って、回収率を評価するバックテストを実行する。

    Args:
        bet_threshold (float): 期待値がこの値を超えた場合に賭ける。デフォルトは1.0。
    """
    print("--- 未知データによるバックテストを開始します ---")

    # --- 1. 準備：モデルと未知データの読み込み ---
    model_path = MODELS_DIR / 'logistic_regression_model.joblib'
    
    print(f"1. 学習済みモデル '{model_path.name}' を読み込みます...")
    try:
        model = load(model_path)
    except FileNotFoundError:
        print(f"エラー: モデルファイルが見つかりません: {model_path}")
        print("先に analytical_aI/models/train.py を実行してモデルを学習してください。")
        return

    print(f"2. 未知データ ('{UNTOUCHED_DATA_DIR.name}') を読み込み、前処理します...")
    df_untouched = load_and_preprocess_data(UNTOUCHED_DATA_DIR)

    if df_untouched.empty:
        print("未知データが見つかりません。train.pyを先に実行してください。")
        return

    # --- 2. 予測：未知データの勝率を予測 ---
    print("3. 未知データの勝率を予測します...")
    
    # train.pyで学習に使ったものと全く同じ特徴量リスト
    features = [
        'popularity', 'jockeyWinRate', 'age', 'weightCarried',
        'horseWeight_val', 'horseWeight_change'
    ]
    features = [f for f in features if f in df_untouched.columns]
    X_untouched = df_untouched[features].fillna(df_untouched[features].mean())

    win_probabilities = model.predict_proba(X_untouched)[:, 1]
    df_untouched['predicted_win_rate'] = win_probabilities

    # --- 3. 判断とシミュレーション：期待値を計算し、賭けを実行 ---
    print(f"4. 期待値が{bet_threshold}を超える馬券をシミュレーションします...")

    # 期待値を計算
    df_untouched['expected_value'] = df_untouched['predicted_win_rate'] * df_untouched['winOdds']

    # 賭けるべき馬を選択
    bets = df_untouched[df_untouched['expected_value'] > bet_threshold]

    if bets.empty:
        print("\n--- バックテスト結果 ---")
        print("賭けるべき馬が見つかりませんでした。")
        return
        
    # --- 4. 結果の評価：回収率を計算 ---
    # 投資額を計算 (1点100円と仮定)
    investment = len(bets) * 100
    
    # 払い戻し額を計算
    winners = bets[bets['isWinner'] == 1]
    total_return = (winners['winOdds'] * 100).sum()

    # 回収率を計算
    roi = (total_return / investment) * 100

    print("\n--- バックテスト結果 ---")
    print(f"対象レース数: {df_untouched['raceId'].nunique()} レース")
    print(f"賭け対象の馬の数: {len(bets)} 頭")
    print(f"投資額: {investment:,.0f} 円")
    print(f"払戻額: {total_return:,.0f} 円")
    print("-------------------------")
    print(f"回収率 (ROI): {roi:.2f} %")
    print("-------------------------")

    print("\n--- 期待値が高かった賭けのサンプル (上位5件) ---")
    print(bets[['raceId', 'horseName', 'popularity', 'winOdds', 'predicted_win_rate', 'expected_value', 'isWinner']]
          .sort_values(by='expected_value', ascending=False).head())


if __name__ == "__main__":
    # 期待値が1.0を超えたら賭ける戦略でバックテストを実行
    main(bet_threshold=1.0)