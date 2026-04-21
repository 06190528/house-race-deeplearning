import sys
import os
import pandas as pd
import lightgbm as lgb
from lightgbm.callback import early_stopping, log_evaluation
from joblib import dump

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

from analytical_aI.config.index import TRAINING_DATA_DIR, MODELS_DIR
from analytical_aI.data.loader import load_and_preprocess_data
from analytical_aI.data.preprocessor import FEATURE_COLS, CAT_COLS
from analytical_aI.data.preprocessor import partition_raw_data


def main():
    # --- Step 0: 生データの分割 ---
    print("0. 生データを「学習用」と「未知データ用」に分割します...")
    partition_raw_data(training_ratio=0.8)

    # --- Step 1: データの読み込みと前処理 ---
    print("\n1. 学習用データの読み込みと前処理を開始します...")
    df, _ = load_and_preprocess_data(TRAINING_DATA_DIR)

    if df.empty:
        print("データが読み込めませんでした。処理を終了します。")
        return
    print(f"> 前処理完了。データ数: {len(df)} 件 / {df['race_id'].nunique()} レース")

    # --- Step 2: 時系列分割（レース単位・古い順に 80% Train / 20% Val）---
    print("\n2. 時系列分割を行います（race_id 昇順で 80/20）...")

    # race_id は "202648040808" 形式で年月日を内包 → ソートで時系列順になる
    unique_races = sorted(df['race_id'].unique())
    split_idx = int(len(unique_races) * 0.8)

    train_races = unique_races[:split_idx]
    val_races   = unique_races[split_idx:]

    train_df = df[df['race_id'].isin(train_races)].copy()
    val_df   = df[df['race_id'].isin(val_races)].copy()

    # 特徴量・ラベル・group 配列を抽出
    available_features = [f for f in FEATURE_COLS if f in df.columns]

    X_train = train_df[available_features]
    y_train = train_df['label']
    group_train = train_df.groupby('race_id', sort=False).size().tolist()

    X_val = val_df[available_features]
    y_val = val_df['label']
    group_val = val_df.groupby('race_id', sort=False).size().tolist()

    print(f"> 学習レース数: {len(train_races)} / 検証レース数: {len(val_races)}")
    print(f"> 学習データ: {len(X_train)} 件 / 検証データ: {len(X_val)} 件")
    print(f"> 使用特徴量: {available_features}")

    # --- Step 3: LGBMRanker の定義と学習 ---
    print("\n3. LambdaRank モデルの学習を開始します...")

    model = lgb.LGBMRanker(
        objective="lambdarank",
        metric="ndcg",
        boosting_type="gbdt",
        n_estimators=1000,
        learning_rate=0.05,
        num_leaves=63,
        importance_type="gain",
        random_state=42,
    )

    model.fit(
        X_train,
        y_train,
        group=group_train,
        eval_set=[(X_val, y_val)],
        eval_group=[group_val],
        eval_at=[3, 5],   # 上位3頭（馬券圏）・5頭（掲示板）のランキング精度を重視
        categorical_feature=CAT_COLS,
        callbacks=[
            early_stopping(stopping_rounds=50),
            log_evaluation(period=50),
        ],
    )

    # --- Step 4: 予測スコアの確認（検証データのサンプル表示）---
    print("\n4. 検証データで予測スコアを確認します...")
    scores = model.predict(X_val)
    val_df = val_df.copy()
    val_df['predicted_score'] = scores

    # レースごとに予測スコア降順でランク付けし、実際の着順と比較
    val_df['predicted_rank'] = val_df.groupby('race_id')['predicted_score'].rank(
        ascending=False, method='min'
    ).astype(int)

    print("\n--- 予測サンプル（最初の1レース）---")
    first_race = val_df['race_id'].iloc[0]
    sample = val_df[val_df['race_id'] == first_race][
        ['race_id', 'rank', 'label', 'predicted_score', 'predicted_rank']
    ].sort_values('predicted_rank')
    print(sample.to_string(index=False))

    # --- Step 5: モデルの保存 ---
    os.makedirs(MODELS_DIR, exist_ok=True)
    model_path = MODELS_DIR / 'lambdarank_model.joblib'
    dump(model, model_path)
    print(f"\n✅ 学習済みモデルを '{model_path}' として保存しました。")
    print(f"   最良イテレーション: {model.best_iteration_}")


if __name__ == "__main__":
    main()
