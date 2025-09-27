
# 外部ファイルから必要な変数や関数をインポート
from config.index import DATA_PATH
from data.loader import load_and_process_race_data  
from data.preprocessor import preprocess_data

def main():
    """
    データ読み込みから前処理までのメイン処理を実行する関数
    """
    print("--- 競馬データ分析処理を開始します ---")

    # 1. ファイルから生のデータを読み込む
    raw_horse_data = load_and_process_race_data(DATA_PATH)

    # データが正常に読み込めたかチェック
    if not raw_horse_data:
        print("データが読み込めませんでした。処理を終了します。")
        return

    # 2. 読み込んだデータを分析用に前処理する
    print("\n--- データの前処理を実行します ---")
    df_processed = preprocess_data(raw_horse_data)

    # 処理後のデータフレームの情報を表示
    print(f"\n✅ 前処理が完了しました。")
    print(f"総データ数: {df_processed.shape[0]}件, 特徴量数: {df_processed.shape[1]}個")
    
    print("\n--- 処理後データの先頭5行 ---")
    print(df_processed.head())


if __name__ == "__main__":
    main()