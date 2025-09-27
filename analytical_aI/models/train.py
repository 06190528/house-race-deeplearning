import sys
import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix
from joblib import dump

# --- プロジェクトルートをPythonの検索パスに追加 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# --- モジュールをインポート ---
from analytical_aI.config.index import TRAINING_DATA_DIR, MODELS_DIR
from analytical_aI.data.loader import load_and_preprocess_data
# ▼▼▼ 変更点①: partition_raw_data をインポート ▼▼▼
from analytical_aI.data.preprocessor import partition_raw_data


def main():
    """
    データ分割からモデル学習までを一貫して行うメイン関数
    """
    # ▼▼▼ 変更点②: 最初のステップとしてデータ分割を実行 ▼▼▼
    # --- Step 0: 生データの分割 ---
    print("0. 生データを「学習用」と「未知データ用」に分割します...")
    partition_raw_data(training_ratio=0.8)


    # --- Step 1: データの読み込みと前処理 ---
    print("\n1. 学習用データの読み込みと前処理を開始します...")
    df = load_and_preprocess_data(TRAINING_DATA_DIR)

    if df.empty:
        print("データが読み込めませんでした。処理を終了します。")
        return
    print(f"> 前処理完了。データ数: {len(df)}件")

    # --- Step 2: データの準備 ---
    print("\n2. 学習データとテストデータに分割します...")
    features = [
        'popularity', 'jockeyWinRate', 'age', 'weightCarried',
        'horseWeight_val', 'horseWeight_change'
    ]
    features = [f for f in features if f in df.columns]
    X = df[features]
    y = df['isWinner']

    X = X.fillna(X.mean())

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    print(f"学習データ数: {len(X_train)}件, テストデータ数: {len(X_test)}件")
    print(f"使用する特徴量: {features}")

    # --- Step 3: モデルの学習 ---
    print("\n3. ロジスティック回帰モデルの学習を開始します...")
    model = LogisticRegression(class_weight='balanced', max_iter=1000)
    model.fit(X_train, y_train)
    print("学習が完了しました。")

    # --- Step 4: 勝率の予測 ---
    print("\n4. テストデータを使って勝率を予測します...")
    win_probabilities = model.predict_proba(X_test)[:, 1]
    
    df_test_result = X_test.copy()
    df_test_result['isWinner_actual'] = y_test.values
    df_test_result['win_probability_predicted'] = win_probabilities
    
    # --- Step 5: モデルの評価 ---
    print("\n5. モデルの性能を評価します...")
    y_pred = (win_probabilities > 0.5).astype(int)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"\nAccuracy (単純な正解率): {accuracy:.4f}")
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, y_pred))
    print("\n--- 予測結果のサンプル (予測確率が高い順) ---")
    print(df_test_result.sort_values(by='win_probability_predicted', ascending=False).head())

    # --- Step 6: モデルの保存 ---
    os.makedirs(MODELS_DIR, exist_ok=True)
    model_filename = MODELS_DIR / 'logistic_regression_model.joblib'
    dump(model, model_filename)
    print(f"\n✅ 学習済みモデルを '{model_filename}' として保存しました。")

if __name__ == "__main__":
    main()