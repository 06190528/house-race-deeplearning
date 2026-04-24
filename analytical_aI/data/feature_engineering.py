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


def add_advanced_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tier1/2/3 高度特徴量を追加する。
    - shift(1) によりデータリーク（未来情報の混入）を防止。
    - Tier1 の欠損（初出走など）は np.nan のまま返す（LightGBM 側で処理）。
    - Tier2 はベイズ平滑化（C=3.0）でノイズを除去。
    - Tier3 はレース内相対化（自身の値 - レース内平均）。
    """
    df = df.copy()
    C = 3.0

    # ---- 共通前処理 ----
    for col in ["rank", "field_size", "weight_carried", "horse_weight", "prize"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    dist_col = next((c for c in ["course_len", "distance"] if c in df.columns), None)
    if dist_col:
        df[dist_col] = pd.to_numeric(df[dist_col], errors="coerce")

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Tier2 用: is_win と global_mean（定数なのでリーク無し）
    if "rank" in df.columns:
        df["__is_win"] = np.where(df["rank"].notna(), (df["rank"] == 1).astype(float), np.nan)
        global_mean = df["__is_win"].mean()
    else:
        global_mean = 0.05

    # ================================================================
    # Tier 1: 物理的・直接的ファクト
    # ================================================================

    # 1. days_since_last_race
    if "date" in df.columns and "horse_id" in df.columns:
        work = df[["horse_id", "race_id", "date"]].sort_values(["horse_id", "race_id"])
        prev_date = work.groupby("horse_id")["date"].transform(lambda x: x.shift(1))
        df["days_since_last_race"] = (work["date"] - prev_date).dt.days.astype(float).reindex(df.index)

    # 2. distance_diff
    if dist_col and "horse_id" in df.columns:
        work = df[["horse_id", "race_id", dist_col]].sort_values(["horse_id", "race_id"])
        prev_dist = work.groupby("horse_id")[dist_col].transform(lambda x: x.shift(1))
        df["distance_diff"] = (work[dist_col] - prev_dist).reindex(df.index)

    # 3. weight_carried_ratio（Tier3 の相対化のベースとしても使用）
    if "weight_carried" in df.columns and "horse_weight" in df.columns:
        df["weight_carried_ratio"] = df["weight_carried"] / df["horse_weight"]

    # 4. prev_rank_ratio（欠損は np.nan のまま、0.5 補完しない）
    if all(c in df.columns for c in ["rank", "field_size", "horse_id"]):
        work = df[["horse_id", "race_id", "rank", "field_size"]].sort_values(["horse_id", "race_id"]).copy()
        work["__rank_ratio"] = work["rank"] / work["field_size"]
        df["prev_rank_ratio"] = (
            work.groupby("horse_id")["__rank_ratio"]
            .transform(lambda x: x.shift(1))
            .reindex(df.index)
        )

    # ================================================================
    # Tier 2: 累積・履歴統計（ベイズ平滑化）
    # ================================================================

    jockey_col = next((c for c in ["jockey_id", "jockey"] if c in df.columns), None)

    # 5. jockey_win_rate_expanding（騎手の全キャリア累積勝率・ベイズ平滑化）
    if jockey_col and "__is_win" in df.columns:
        work = df[[jockey_col, "race_id", "__is_win"]].sort_values([jockey_col, "race_id"])
        rides = work.groupby(jockey_col).cumcount()          # 当該レース前の騎乗数
        wins = work.groupby(jockey_col)["__is_win"].transform(
            lambda x: x.shift(1).fillna(0).cumsum()         # 当該レース前の累積勝利数
        )
        df["jockey_win_rate_expanding"] = (
            ((wins + C * global_mean) / (rides + C)).reindex(df.index)
        )

    # 6. horse_track_win_rate（馬×芝/ダ 累積勝率・ベイズ平滑化）
    if all(c in df.columns for c in ["horse_id", "track_type"]) and "__is_win" in df.columns:
        work = df[["horse_id", "track_type", "race_id", "__is_win"]].sort_values(
            ["horse_id", "track_type", "race_id"]
        )
        rides = work.groupby(["horse_id", "track_type"]).cumcount()
        wins = work.groupby(["horse_id", "track_type"])["__is_win"].transform(
            lambda x: x.shift(1).fillna(0).cumsum()
        )
        df["horse_track_win_rate"] = (
            ((wins + C * global_mean) / (rides + C)).reindex(df.index)
        )

    # 7. prev_prize_median_5（近5走の獲得賞金中央値）
    if "prize" in df.columns and "horse_id" in df.columns:
        work = df[["horse_id", "race_id", "prize"]].sort_values(["horse_id", "race_id"])
        df["prev_prize_median_5"] = (
            work.groupby("horse_id")["prize"]
            .transform(lambda x: x.shift(1).rolling(window=5, min_periods=1).median())
            .reindex(df.index)
        )

    # ================================================================
    # Tier 3: レース内相対化（LambdaRank 用 偏差特徴量）
    # ================================================================
    for src_col, new_col in [
        ("weight_carried_ratio", "weight_carried_ratio_relative"),
        ("days_since_last_race", "days_since_last_race_relative"),
        ("jockey_win_rate_expanding", "jockey_win_rate_expanding_relative"),
    ]:
        if src_col in df.columns and "race_id" in df.columns:
            df[new_col] = df[src_col] - df.groupby("race_id")[src_col].transform("mean")

    df.drop(columns=["__is_win"], errors="ignore", inplace=True)
    return df


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
