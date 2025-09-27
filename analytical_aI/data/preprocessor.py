import pandas as pd

def preprocess_data(raw_data: list[dict]) -> pd.DataFrame:
    """
    生のレースデータを分析可能なDataFrameに前処理する関数。
    文字列を数値に変換し、isWinnerフラグを追加する。

    Args:
        raw_data (list[dict]): load_and_process_race_dataで読み込んだデータ

    Returns:
        pd.DataFrame: 前処理済みのデータフレーム
    """
    if not raw_data:
        return pd.DataFrame()

    # 1. リストからPandasのDataFrameを作成
    df = pd.DataFrame(raw_data)

    # 2. 必要な列を数値型に変換（変換できないものはNaNになる）
    numeric_cols = ['rank', 'popularity', 'winOdds', 'horseNumber']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 3. 不正なデータ（NaNやオッズが0）を持つ行を削除
    df.dropna(subset=numeric_cols, inplace=True)
    df = df[df['winOdds'] > 0]
    
    # rankを整数型に変換
    df['rank'] = df['rank'].astype(int)

    # 4. isWinner列を作成（rankが1なら1、それ以外は0）
    df['isWinner'] = (df['rank'] == 1).astype(int)
    
    # 5. 必要な列だけを整理して返す
    final_cols = [
        'raceId', 'horseNumber', 'horseName', 'rank',
        'popularity', 'winOdds', 'isWinner'
    ]
    return df[final_cols]


# --- 実行例 ---
if __name__ == '__main__':
    # このスクリプトが analytical_ai/data/ にあると仮定
    # 実際のパスに合わせて調整してください
    data_directory = '../../functions/scrape/racedata'
    
    raw_horse_data = load_and_process_race_data(data_directory)
    
    if raw_horse_data:
        df_processed = preprocess_data(raw_horse_data)
        print("\n--- Preprocessed Data (first 5 rows) ---")
        print(df_processed.head())
