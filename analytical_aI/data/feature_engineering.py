import pandas as pd


def calculate_jockey_win_rates(raw_data: list[dict], min_rides: int = 20) -> dict:
    """
    全レースデータから騎手ごとの勝率を計算する。

    Args:
        raw_data: load_and_process_race_data で読み込んだ生の全レースデータ
        min_rides: 勝率を計算するための最低騎乗回数（デフォルト20）

    Returns:
        dict: 騎手ID をキー、勝率を値とする辞書
    """
    jockey_stats: dict[str, dict] = {}

    for horse in raw_data:
        # 新フォーマット: jockey_id / 旧フォーマット: jockey の両方に対応
        jockey = horse.get("jockey_id") or horse.get("jockey")
        if not jockey:
            continue

        if jockey not in jockey_stats:
            jockey_stats[jockey] = {"rides": 0, "wins": 0}

        jockey_stats[jockey]["rides"] += 1

        # rank は新フォーマットでは int、旧フォーマットでは str の場合がある
        rank = horse.get("rank")
        try:
            if int(rank) == 1:
                jockey_stats[jockey]["wins"] += 1
        except (TypeError, ValueError):
            pass

    jockey_win_rates: dict[str, float] = {}
    for jockey, stats in jockey_stats.items():
        if stats["rides"] >= min_rides:
            jockey_win_rates[jockey] = stats["wins"] / stats["rides"]
        else:
            jockey_win_rates[jockey] = 0.0

    return jockey_win_rates

def calculate_last3f_zscore(df: pd.DataFrame) -> pd.Series:
    """馬の過去レースにおける上がり3F Z-scoreの平均を返す（当該レースは含まない）。"""
    work = df[["horse_id", "race_id", "last_3f"]].copy()
    work["last_3f"] = pd.to_numeric(work["last_3f"], errors="coerce")

    def zscore(x):
        std = x.std()
        if std == 0 or pd.isna(std):
            return pd.Series(0.0, index=x.index)
        return (x - x.mean()) / std

    work["_z_tmp"] = work.groupby("race_id")["last_3f"].transform(zscore)

    work_sorted = work.sort_values(["horse_id", "race_id"])
    historical = work_sorted.groupby("horse_id")["_z_tmp"].transform(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).mean()
    )

    return historical.reindex(work.index)

# --- ここに他の特徴量作成関数も追加していく ---
# def calculate_trainer_win_rates(raw_data, min_races=20):
#   ...
