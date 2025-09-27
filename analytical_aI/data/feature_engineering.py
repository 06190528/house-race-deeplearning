import pandas as pd
# 特徴量を生成するための工場
def calculate_jockey_win_rates(raw_data: list[dict], min_rides: int = 20) -> dict:
    """
    全レースデータから騎手ごとの勝率を計算する。

    Args:
        raw_data (list[dict]): loaderで読み込んだ生の全レースデータ。
        min_rides (int, optional): 勝率を計算するための最低騎乗回数。デフォルトは20回。

    Returns:
        dict: 騎手名をキー、勝率を値とする辞書。
    """
    # 1. まず全データから騎手ごとの成績を集計
    jockey_stats = {}
    for horse in raw_data:
        jockey = horse.get('jockey')
        if not jockey:
            continue  # 騎手名がないデータはスキップ

        if jockey not in jockey_stats:
            jockey_stats[jockey] = {'rides': 0, 'wins': 0}

        jockey_stats[jockey]['rides'] += 1
        if horse.get('rank') == '1':
            jockey_stats[jockey]['wins'] += 1

    # 2. 騎手ごとの勝率を計算
    jockey_win_rates = {}
    for jockey, stats in jockey_stats.items():
        if stats['rides'] >= min_rides:
            jockey_win_rates[jockey] = stats['wins'] / stats['rides']
        else:
            jockey_win_rates[jockey] = 0.0  # 騎乗回数が少ない場合は0.0とする

    return jockey_win_rates


# --- ここに他の特徴量作成関数も追加していく ---
# def calculate_trainer_win_rates(raw_data, min_races=20):
#   ...