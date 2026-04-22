import sys
import os
import numpy as np
import pandas as pd
from joblib import load
import optuna  # 追加: 最適化ライブラリ

# --- プロジェクトルートをPythonの検索パスに追加 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# --- モジュールをインポート ---
from analytical_aI.config.index import MODELS_DIR, UNTOUCHED_DATA_DIR
from analytical_aI.data.loader import load_and_preprocess_data
from analytical_aI.data.preprocessor import FEATURE_COLS

# ログを少し静かにする（Optunaの出力が多すぎるのを防ぐ）
optuna.logging.set_verbosity(optuna.logging.WARNING)

def calculate_roi(df_untouched: pd.DataFrame, bet_threshold: float, win_rate_threshold: float, race_budget: int = 100):
    """
    【高速化版】ループ(iterrows)を排除し、ベクトル演算のみでシミュレーションを行う
    """
    # 1. 閾値でデータ全体を一括フィルタリング
    mask = (df_untouched['expected_value'] > bet_threshold) & (df_untouched['predicted_win_rate'] > win_rate_threshold)
    value_bets = df_untouched[mask].copy()

    # 買うべき馬が1頭もいない場合は即リターン
    if value_bets.empty:
        return 0.0, 0, 0, 0, 0

    # 2. 賭け対象となった馬の「レースごとの予測勝率の合計」を一括計算
    prob_sums = value_bets.groupby('race_id')['predicted_win_rate'].transform('sum')

    # 3. 各馬への賭け金（bet_amount）を一括計算
    value_bets['bet_amount'] = race_budget * (value_bets['predicted_win_rate'] / prob_sums)

    # 4. 払い戻し（return）を一括計算（label == 3 の場合のみ 賭け金 × オッズ、それ以外は 0）
    value_bets['return'] = np.where(value_bets['label'] == 3, value_bets['bet_amount'] * value_bets['odds'], 0.0)

    # 5. 結果の集計
    bet_races = value_bets['race_id'].nunique()
    num_bets = len(value_bets)
    total_investment = bet_races * race_budget  # 賭けたレース数 × レースごとの予算
    total_return = value_bets['return'].sum()

    roi = (total_return / total_investment) * 100 if total_investment > 0 else 0.0

    return roi, bet_races, num_bets, total_investment, total_return

def main():
    print("--- ベッティングロジックの自動最適化を開始します ---")

    # --- 1. モデルとデータの読み込み（1回だけ実行） ---
    model_path = MODELS_DIR / 'lambdarank_model.joblib'
    print("1. モデルとデータを読み込み中...")
    try:
        model = load(model_path)
    except FileNotFoundError:
        print(f"エラー: モデルファイルが見つかりません。")
        return

    df_untouched, _ = load_and_preprocess_data(UNTOUCHED_DATA_DIR)
    if df_untouched.empty:
        return

    available_features = [f for f in FEATURE_COLS if f in df_untouched.columns]
    X_untouched = df_untouched[available_features]

    # --- 2. スコア予測と期待値計算（これも1回だけ実行でOK） ---
    print("2. スコア予測と期待値(EV)を計算中...")
    df_untouched['predicted_score'] = model.predict(X_untouched)

    def softmax(x):
        e_x = np.exp(x - np.max(x))
        return e_x / e_x.sum()

    df_untouched['predicted_win_rate'] = df_untouched.groupby('race_id')['predicted_score'].transform(
        lambda x: softmax(x.values)
    )
    df_untouched['expected_value'] = df_untouched['predicted_win_rate'] * df_untouched['odds']

    total_races = df_untouched['race_id'].nunique()

    # race_id 昇順でソートし、前半50%をOptuna用、後半50%をテスト用に分割
    all_races = sorted(df_untouched['race_id'].unique())
    split = len(all_races) // 2
    df_optuna = df_untouched[df_untouched['race_id'].isin(all_races[:split])]
    df_test   = df_untouched[df_untouched['race_id'].isin(all_races[split:])]
    optuna_races = len(all_races[:split])


    # --- 3. Optunaによる最適化 ---
    print("3. Optunaで最適な閾値(bet_threshold, win_rate_threshold)を探索中...")

    def objective(trial):
        # 探索するパラメータの範囲を定義
        bet_threshold = trial.suggest_float("bet_threshold", 1.0, 2.0)
        win_rate_threshold = trial.suggest_float("win_rate_threshold", 0.01, 0.30)
        
        roi, bet_races, _, _, _ = calculate_roi(df_optuna, bet_threshold, win_rate_threshold)
        
        # 例外処理: 賭けるレースが極端に少ない（例えば全体の5%未満）場合は、
        # まぐれ当たりの過学習を防ぐためにROIをペナルティとして0にする
        if bet_races < (total_races * 0.25):
            return 0.0
            
        return roi

    # ROIを「最大化(maximize)」する方向で探索
    study = optuna.create_study(direction="maximize")
    
    # 200回シミュレーションを回す（数秒〜数十秒で終わります）
    study.optimize(objective, n_trials=200)

    best_params = study.best_params
    best_roi = study.best_value

    print("\n--- 真の未知データ(df_test)での True ROI ---")
    roi, bet_races, num_bets, total_investment, total_return = calculate_roi(
        df_test,
        best_params['bet_threshold'],
        best_params['win_rate_threshold']
    )
    test_races = df_test['race_id'].nunique()
    print(f"対象レース数: {test_races} レース")
    print(f"賭け対象レース数: {bet_races} レース ({ (bet_races/test_races)*100:.2f} %)")
    print(f"賭け対象の馬の総数: {num_bets} 頭")
    print(f"総投資額: {total_investment:,.0f} 円")
    print(f"総払戻額: {total_return:,.0f} 円")
    print("-------------------------")
    print(f"回収率 (ROI): {roi:.2f} %")
    print("-------------------------")

if __name__ == "__main__":
    main()