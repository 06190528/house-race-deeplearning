import pandas as pd
import numpy as np


def calculate_jockey_win_rate(df: pd.DataFrame, window: int | None = None) -> pd.Series:
    """
    騎手の過去勝率を返す（当該レースは含まない）。

    shift(1) により自然にリーク防止。
    window: 直近N走で集計（None なら全期間累計 ← デフォルト）
    20走未満のジョッキーは 0.0 で補完。
    """
    jockey_col = "jockey_id" if "jockey_id" in df.columns else "jockey"
    work = df[[jockey_col, "race_id", "rank"]].copy()
    work["rank"] = pd.to_numeric(work["rank"], errors="coerce")
    work["is_win"] = (work["rank"] == 1).astype(int)
    work_sorted = work.sort_values([jockey_col, "race_id"])

    if window is None:
        rides = work_sorted.groupby(jockey_col).cumcount()
        wins  = work_sorted.groupby(jockey_col)["is_win"].transform(
            lambda x: x.shift(1).cumsum().fillna(0)
        )
    else:
        wins  = work_sorted.groupby(jockey_col)["is_win"].transform(
            lambda x: x.shift(1).rolling(window=window, min_periods=1).sum().fillna(0)
        )
        rides = work_sorted.groupby(jockey_col)["is_win"].transform(
            lambda x: x.shift(1).rolling(window=window, min_periods=1).count().fillna(0)
        )

    result = np.where(rides >= 20, wins / rides, 0.0)
    return pd.Series(result, index=work_sorted.index).reindex(work.index)

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

def calculate_prev_time_diff(df: pd.DataFrame) -> pd.Series:
    """前走の1着馬とのタイム差（秒）を返す（当該レースは含まない）。"""
    work = df[["horse_id", "race_id", "time", "label"]].copy()
    work["time"] = pd.to_numeric(work["time"], errors="coerce")

    # レースごとの1着タイム（label==3 が1着）
    winner_time = work[work["label"] == 3].groupby("race_id")["time"].first()
    work["winner_time"] = work["race_id"].map(winner_time)
    work["time_diff"] = work["time"] - work["winner_time"]

    work_sorted = work.sort_values(["horse_id", "race_id"])
    prev_diff = work_sorted.groupby("horse_id")["time_diff"].transform(
        lambda x: x.shift(1)
    )

    return prev_diff.reindex(work.index).fillna(10.0)


def calculate_prev_rank_ratio(df: pd.DataFrame) -> pd.Series:
    """前走の着順割合（rank / field_size）を返す。初出走は 0.5 で補完。"""
    work = df[["horse_id", "race_id", "rank", "field_size"]].copy()
    work["rank"] = pd.to_numeric(work["rank"], errors="coerce")
    work["field_size"] = pd.to_numeric(work["field_size"], errors="coerce")
    work["rank_ratio"] = work["rank"] / work["field_size"]

    work_sorted = work.sort_values(["horse_id", "race_id"])
    prev_ratio = work_sorted.groupby("horse_id")["rank_ratio"].transform(
        lambda x: x.shift(1)
    )

    return prev_ratio.reindex(work.index).fillna(0.5)


def calculate_jockey_track_win_rate(df: pd.DataFrame) -> pd.Series:
    """騎手×コース種別（芝/ダ）の過去勝率を返す（当該レースは含まない）。"""
    jockey_col = "jockey_id" if "jockey_id" in df.columns else "jockey"

    work = df[[jockey_col, "track_type", "race_id", "rank"]].copy()
    work["rank"] = pd.to_numeric(work["rank"], errors="coerce")
    work["is_win"] = (work["rank"] == 1).astype(int)

    work_sorted = work.sort_values([jockey_col, "track_type", "race_id"])

    rides = work_sorted.groupby([jockey_col, "track_type"]).cumcount()
    wins = work_sorted.groupby([jockey_col, "track_type"])["is_win"].transform(
        lambda x: x.shift(1).cumsum().fillna(0)
    )

    result = np.where(rides > 0, wins / rides, 0.0)
    return pd.Series(result, index=work_sorted.index).reindex(work.index)


def calculate_historical_pci(df: pd.DataFrame) -> pd.DataFrame:
    """馬の過去レースにおける自身のPCIと、レース全体PCI(RPCI)の近走平均を返す。"""
    work = df[["horse_id", "race_id", "time", "last_3f", "distance"]].copy()

    work["time"] = pd.to_numeric(work["time"], errors="coerce")
    work["last_3f"] = pd.to_numeric(work["last_3f"], errors="coerce")
    work["distance"] = pd.to_numeric(work["distance"], errors="coerce")

    valid_mask = (work["distance"] > 600) & (work["time"] > 0) & (work["last_3f"] > 0)

    work["pace_dist"] = work["distance"] - 600
    work["pace_time"] = work["time"] - work["last_3f"]

    work["pace_3f"] = np.where(
        valid_mask & (work["pace_dist"] > 0),
        (work["pace_time"] / work["pace_dist"]) * 600,
        np.nan
    )
    work["pci_raw"] = np.where(
        valid_mask & (work["last_3f"] > 0),
        (work["pace_3f"] / work["last_3f"]) * 50,
        np.nan
    )
    work["rpci_raw"] = work.groupby("race_id")["pci_raw"].transform("mean")

    work_sorted = work.sort_values(["horse_id", "race_id"])

    past_pci = work_sorted.groupby("horse_id")["pci_raw"].transform(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).mean()
    )
    past_rpci = work_sorted.groupby("horse_id")["rpci_raw"].transform(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).mean()
    )

    df["past_pci"] = past_pci.reindex(work.index).fillna(50.0)
    df["past_rpci"] = past_rpci.reindex(work.index).fillna(50.0)

    return df
