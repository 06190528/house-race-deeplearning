import pandas as pd
import numpy as np
import os
import glob
import random
import shutil

from analytical_aI.config.index import DATA_PATH, UNTOUCHED_DATA_DIR, TRAINING_DATA_DIR


# ▼▼▼ 変更点① ▼▼▼
# 外部の関数をインポート
from analytical_aI.data.feature_engineering import calculate_jockey_win_rates

# ▼▼▼ 変更点② ▼▼▼
# 引数から jockey_win_rates を削除
def preprocess_data(raw_data: list[dict]) -> pd.DataFrame:
    """
    生のレースデータを分析可能なDataFrameに前処理する。
    この関数内で特徴量エンジニアリングも呼び出す。
    """
    if not raw_data:
        return pd.DataFrame()

    # ▼▼▼ 変更点③ ▼▼▼
    # 関数内で騎手勝率を計算する
    print("Calculating jockey win rates...")
    jockey_win_rates = calculate_jockey_win_rates(raw_data)

    df = pd.DataFrame(raw_data)

    # --- 1. 基本的な数値列の変換 ---
    numeric_cols = [
        'rank', 'frameNumber', 'horseNumber', 'popularity', 
        'weightCarried', 'winOdds', 'last3Furlongs', 'prizeMoney'
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # --- 2. 複合データの分割と変換 ---
    df['sex'] = df['sexAndAge'].str[0].map({'牝': 0, '牡': 1, 'セ': 2}).fillna(-1).astype(int)
    df['age'] = pd.to_numeric(df['sexAndAge'].str[1:], errors='coerce')

    weight_data = df['horseWeight'].str.extract(r'(\d+)\(([-+]?\d+)\)').astype(float)
    df['horseWeight_val'] = weight_data[0]
    df['horseWeight_change'] = weight_data[1]

    def time_to_seconds(time_str):
        if pd.isna(time_str): return np.nan
        parts = str(time_str).split(':')
        try:
            if len(parts) == 2: return float(parts[0]) * 60 + float(parts[1])
            elif len(parts) == 1: return float(parts[0])
        except (ValueError, TypeError): return np.nan
        return np.nan
        
    df['time_seconds'] = df['time'].apply(time_to_seconds)

    # --- 3. 新しい特徴量の追加 ---
    df['isWinner'] = (df['rank'] == 1).astype(int)
    df['jockeyWinRate'] = df['jockey'].map(jockey_win_rates).fillna(0)

    # --- 4. 不要な列の削除と最終クリーンアップ ---
    df.drop(columns=['sexAndAge', 'horseWeight', 'time'], inplace=True)
    df.dropna(subset=['rank', 'popularity', 'winOdds', 'horseNumber'], inplace=True)
    df = df[df['winOdds'] > 0]
    df['rank'] = df['rank'].astype(int)

    return df


def partition_raw_data(training_ratio: float = 0.8) -> None:
    """
    元の生データセットを「学習用」と「未知データ用」に分割し、
    それぞれの専用フォルダにファイルをコピーする。
    """
    print(f"\n--- データを学習用と未知用に分割します ---")

    # 1. 各フォルダを一旦空にして再作成
    for dir_path in [UNTOUCHED_DATA_DIR, TRAINING_DATA_DIR]:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        os.makedirs(dir_path)

    # 2. 全てのjsonファイルリストを取得してシャッフル
    all_files = glob.glob(os.path.join(DATA_PATH, '*.json'))
    random.shuffle(all_files)

    # 3. 学習用と未知用にファイルリストを分割
    split_index = int(len(all_files) * training_ratio)
    training_files = all_files[:split_index]
    untouched_files = all_files[split_index:]

    # 4. それぞれの専用フォルダにファイルをコピーする
    print("学習用データをコピー中...")
    for f in training_files:
        shutil.copy(f, TRAINING_DATA_DIR)
        
    print("未知データをコピー中...")
    for f in untouched_files:
        shutil.copy(f, UNTOUCHED_DATA_DIR)
        
    print(f"学習用データ: {len(training_files)}レース分を '{TRAINING_DATA_DIR.name}' にコピーしました。")
    print(f"未知データ: {len(untouched_files)}レース分を '{UNTOUCHED_DATA_DIR.name}' にコピーしました。")
    print("元のデータフォルダは変更されていません。")
