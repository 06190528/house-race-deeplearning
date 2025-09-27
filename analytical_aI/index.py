import sys
import pprint
# 外部ファイルから必要な変数や関数をインポート
from config.index import DATA_PATH
from data.loader import load_and_process_race_data  
from data.preprocessor import preprocess_data

def main():
    

    pprint.pprint(sys.path)
    # raw_horse_data = load_and_process_race_data(DATA_PATH)
    # if not raw_horse_data:
    #     print("データが読み込めませんでした。処理を終了します。")
    #     return

    # print("\n--- データの前処理を実行します ---")
    # df_processed = preprocess_data(raw_horse_data)

    # print(f"\n✅ 前処理が完了しました。")
    # print(f"総データ数: {df_processed.shape[0]}件, 特徴量数: {df_processed.shape[1]}個")
    
    # print("\n--- 処理後データの先頭5行 ---")
    # print(df_processed.head())


if __name__ == "__main__":
    main()