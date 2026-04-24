import os
import json
import pandas as pd

from .preprocessor import preprocess_data


def load_and_process_race_data(data_path: str) -> list[dict]:
    """
    指定されたディレクトリから全てのレースデータを読み込み、
    race_info の各フィールドを各馬のレコードに展開して単一リストに変換する。

    JSONフォーマット:
        { "race_id": "...", "race_info": {...}, "horses": [...] }
    """
    print(f"📂 Reading data from: {data_path}")
    all_horse_data = []

    try:
        files = os.listdir(data_path)

        for file_name in files:
            if not file_name.endswith(".json"):
                continue

            file_path = os.path.join(data_path, file_name)
            with open(file_path, "r", encoding="utf-8") as f:
                single_race_data = json.load(f)

            # --- 新フォーマット: {"race_id": ..., "race_info": {...}, "horses": [...]} ---
            if isinstance(single_race_data, dict) and "horses" in single_race_data:
                race_id = single_race_data.get("race_id", os.path.splitext(file_name)[0])
                race_info = single_race_data.get("race_info", {})

                for horse_result in single_race_data["horses"]:
                    record = horse_result.copy()
                    record["race_id"] = race_id
                    # race_info のフィールドをフラットに追加
                    record["track_type"] = race_info.get("track_type")
                    record["direction"] = race_info.get("direction")
                    record["distance"] = race_info.get("distance")
                    record["weather"] = race_info.get("weather")
                    record["track_condition"] = race_info.get("track_condition")
                    all_horse_data.append(record)

            # --- 旧フォーマット: [horse, horse, ...] の配列 ---
            elif isinstance(single_race_data, list):
                race_id = os.path.splitext(file_name)[0]
                for horse_result in single_race_data:
                    horse_result["race_id"] = race_id
                    all_horse_data.append(horse_result)

    except FileNotFoundError:
        print(f"[Error] Directory not found: {data_path}")
    except Exception as e:
        print(f"[Error] Failed to read or process data: {e}")

    print(f"✅ Successfully loaded data for {len(all_horse_data)} horses.")
    return all_horse_data


def load_and_preprocess_data(data_path: str) -> tuple[pd.DataFrame, list[int]]:
    """データ読み込みから前処理まで一括で行う。"""
    raw_data = load_and_process_race_data(data_path)

    if not raw_data:
        print("生データが見つからなかったため、空のDataFrameを返します。")
        return pd.DataFrame(), []

    df, group_data = preprocess_data(raw_data)
    return df, group_data


def load_and_split_data(data_path: str, train_ratio: float = 0.8) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    全データを一括で前処理したあと、race_id昇順でtrain/unseenに分割して返す。
    騎手勝率は shift(1) ベースのローリング集計のため全データで一括処理してよい。

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: (学習用df, 未知データdf)
    """
    df, _ = load_and_preprocess_data(data_path)
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    unique_races = sorted(df['race_id'].unique())
    split_idx    = int(len(unique_races) * train_ratio)

    train_df  = df[df['race_id'].isin(unique_races[:split_idx])].copy()
    unseen_df = df[df['race_id'].isin(unique_races[split_idx:])].copy()

    print(f"> 学習用: {train_df['race_id'].nunique()} レース / 未知データ: {unseen_df['race_id'].nunique()} レース")
    return train_df, unseen_df
