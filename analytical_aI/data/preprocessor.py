import pandas as pd
import numpy as np

from analytical_aI.data.feature_engineering import (
    calculate_jockey_win_rate,
    calculate_last3f_zscore,
    calculate_historical_pci,
    calculate_jockey_track_win_rate,
    calculate_prev_time_diff,
    add_advanced_features,
)


# ---------------------------------------------------------------------------
# カテゴリ変数カラム定義（LightGBMが category 型を直接扱うため One-Hot 不要）
# ---------------------------------------------------------------------------
CAT_COLS = ["sex", "track_type", "track_condition", "weather", "direction"]

# ---------------------------------------------------------------------------
# 学習に使う特徴量（winOdds / popularity は除外 ― バックテスト時のみ使用）
# ---------------------------------------------------------------------------
FEATURE_COLS = [
    # --- ベース特徴量 ---
    "age",
    "sex",               # カテゴリ
    "weight_carried",    # 斤量
    "horse_weight",      # 馬体重
    "weight_change",     # 体重増減
    "jockey_win_rate",   # 騎手勝率（全キャリア累積、旧実装）
    "track_type",        # 芝 / ダ（カテゴリ）
    "distance",          # 距離
    "track_condition",   # 馬場状態（カテゴリ）
    "weather",           # 天候（カテゴリ）
    "direction",         # コース方向（カテゴリ）
    "last_3f_zscore",    # 上がり3F 過去3走Z-score平均
    "load_ratio",        # 斤量 / 馬体重（体重比負担率）
    "field_size",        # 出走頭数
    "avg_odds",          # レース内単勝平均オッズ（混戦度）
    "jockey_track_win_rate",      # 騎手×コース種別（芝/ダ）の過去勝率
    "prev_time_diff",             # 前走の1着馬とのタイム差（秒）
    "prev_rank_ratio",            # 前走の着順割合（NaN版: 初出走は欠損）
    "jockey_win_rate_relative",   # 騎手勝率のレース内偏差（旧実装ベース）
    # --- Tier 1: 物理的・直接的ファクト ---
    "days_since_last_race",       # ローテーション（日数）
    "distance_diff",              # 距離変化（延長+・短縮-）
    # --- Tier 2: 累積・履歴統計（ベイズ平滑化） ---
    "jockey_win_rate_expanding",  # 騎手の全キャリア累積勝率（ベイズ平滑化）
    "horse_track_win_rate",       # 馬×芝/ダ 累積勝率（ベイズ平滑化）
    "prev_prize_median_5",        # 近5走の獲得賞金中央値
    # --- Tier 3: レース内相対化 ---
    "weight_carried_ratio_relative",      # 斤量体重比のレース内偏差
    "days_since_last_race_relative",      # ローテーションのレース内偏差
    "jockey_win_rate_expanding_relative", # 騎手勝率（ベイズ）のレース内偏差
]


def _get_relevance_score(rank) -> int:
    """着順を LambdaRank 用の relevance score に変換する。"""
    try:
        r = int(rank)
        if r == 1:
            return 3
        elif r == 2:
            return 2
        elif r == 3:
            return 1
        else:
            return 0
    except (TypeError, ValueError):
        # "中"（中止）"取"（取消）などの文字列は 0 扱い
        return 0


def preprocess_data(raw_data: list[dict]) -> tuple[pd.DataFrame, list[int]]:
    """生のレースデータを LambdaRank 学習用 DataFrame に変換する。"""
    if not raw_data:
        return pd.DataFrame(), []

    # --- 2. DataFrame 化 ---
    df = pd.DataFrame(raw_data)

    # --- 3. 数値型変換 ---
    numeric_cols = [
        "rank", "horse_number", "frame_number",
        "age", "weight_carried", "horse_weight", "weight_change",
        "odds", "popularity", "distance",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- 4. sex のエンコード（カテゴリ変数として扱うため文字列を保持） ---
    # LightGBM category 型で直接扱う → 日本語文字列のまま category にキャスト

    # --- 5. 騎手勝率を付与（直近50走ローリング、shift(1)でリーク防止）---
    df["jockey_win_rate"] = calculate_jockey_win_rate(df)

    # --- 6. 目的変数: relevance score（LambdaRank 用） ---
    df["label"] = df["rank"].apply(_get_relevance_score)

    # --- 7. 不要行の除去 ---
    # rank が解釈不能（取消・除外など）は既に label=0 だが、
    # odds が 0 / NaN の行は期待値計算で使えないため除外
    df = df[df["odds"].notna() & (df["odds"] > 0)].copy()
    df.dropna(subset=["rank", "horse_number"], inplace=True)
    df["rank"] = df["rank"].astype(int)

    # --- . 上がり3ハロンを付与 ---
    df["last_3f_zscore"] = calculate_last3f_zscore(df)

    # --- . 斤量体重比を付与 ---
    df["load_ratio"] = df["weight_carried"] / df["horse_weight"]

    # --- . 出走頭数を付与 ---
    df["field_size"] = df.groupby("race_id")["race_id"].transform("count")

    # --- . 単勝平均オッズを付与（レースの混戦度） ---
    df["avg_odds"] = df.groupby("race_id")["odds"].transform("mean")

    # # --- . PCI・RPCI の過去近走平均を付与（精度低下のため一時無効化）---
    # df = calculate_historical_pci(df)

    # --- . 騎手×コース種別の過去勝率を付与 ---
    df["jockey_track_win_rate"] = calculate_jockey_track_win_rate(df)

    # --- . 前走の1着馬とのタイム差を付与 ---
    df["prev_time_diff"] = calculate_prev_time_diff(df)

    # --- . Tier1/2/3 高度特徴量を一括付与（prev_rank_ratio の NaN版も内包） ---
    df = add_advanced_features(df)

    # --- . 相対特徴量: 騎手勝率のレース内偏差（旧実装 jockey_win_rate ベース） ---
    df["jockey_win_rate_relative"] = (
        df["jockey_win_rate"] - df.groupby("race_id")["jockey_win_rate"].transform("mean")
    )

    # --- 8. カテゴリ変数を category 型にキャスト ---
    for col in CAT_COLS:
        if col in df.columns:
            df[col] = df[col].astype("category")

    # --- 9. 数値特徴量の欠損値を平均補完 ---
    num_feature_cols = [c for c in FEATURE_COLS if c in df.columns and c not in CAT_COLS]
    df[num_feature_cols] = df[num_feature_cols].fillna(df[num_feature_cols].mean())

    # --- 10. race_id でソート（LambdaRank の絶対条件） ---
    df = df.sort_values(by=["race_id", "horse_number"]).reset_index(drop=True)

    # --- 11. group 配列の生成 ---
    group_data: list[int] = df.groupby("race_id", sort=False).size().tolist()

    print(f"前処理完了: {len(df)} 件 / {df['race_id'].nunique()} レース")
    return df, group_data


