import sys
import os
import pandas as pd
from joblib import load

# --- プロジェクトルートをPythonの検索パスに追加 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# --- モジュールをインポート ---
# ▼▼▼ 変更点①: configと新しいローダー関数をインポート ▼▼▼
from analytical_aI.config.index import MODELS_DIR, UNTOUCHED_DATA_DIR
from analytical_aI.data.loader import load_and_preprocess_data


# ▼▼▼ 変更点②: 引数をなくす ▼▼▼
def predict_on_untouched_data() -> pd.DataFrame:
    """
    学習済みモデルを読み込み、未知データ(untouched_data)の勝率を予測する。
    """
    model_path = MODELS_DIR / 'logistic_regression_model.joblib'
    
    print(f"1. 学習済みモデル '{model_path.name}' を読み込みます...")
    try:
        model = load(model_path)
    except FileNotFoundError:
        print(f"エラー: モデルファイルが見つかりません: {model_path}")
        return pd.DataFrame()

    # ▼▼▼ 変更点③: configのパスを使い、新しいローダー関数でデータを一括取得 ▼▼▼
    print(f"2. 未知データ ('{UNTOUCHED_DATA_DIR.name}') を読み込み、前処理します...")
    df_new = load_and_preprocess_data(UNTOUCHED_DATA_DIR)

    if df_new.empty:
        print("予測対象のデータがありません。")
        return pd.DataFrame()

    print("3. 新しいデータの勝率を予測します...")
    features = [
        'popularity', 'jockeyWinRate', 'age', 'weightCarried',
        'horseWeight_val', 'horseWeight_change'
    ]
    features = [f for f in features if f in df_new.columns]
    X_new = df_new[features].fillna(df_new[features].mean())

    win_probabilities = model.predict_proba(X_new)[:, 1]
    df_new['win_probability_predicted'] = win_probabilities
    
    return df_new

# --- 実行例 ---
if __name__ == '__main__':
    prediction_result = predict_on_untouched_data()

    if not prediction_result.empty:
        print("\n--- 予測結果 ---")
        # 必要な列だけを表示し、予測確率が高い順にソート
        display_cols = ['raceId', 'horseName', 'popularity', 'winOdds', 'win_probability_predicted']
        print(prediction_result[display_cols].sort_values(by='win_probability_predicted', ascending=False).head(10))