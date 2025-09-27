import os
import json


def load_and_process_race_data(data_path: str) -> list[dict]:
    """
    指定されたディレクトリから全てのレースデータを読み込み、
    raceIdを各レコードに追加して単一のリストに変換する関数。

    Args:
        data_path (str): データディレクトリへのパス

    Returns:
        list[dict]: 全ての馬の成績データを含む単一のリスト
    """
    print(f"📂 Reading data from: {data_path}")
    all_horse_data = []

    try:
        # ディレクトリ内の全ファイル名を取得
        files = os.listdir(data_path)

        for file_name in files:
            if file_name.endswith(".json"):
                file_path = os.path.join(data_path, file_name)

                with open(file_path, "r", encoding="utf-8") as f:
                    single_race_data = json.load(f)

                # ファイル名から 'race_id' を取得 (拡張子 .json を除去)
                race_id = os.path.splitext(file_name)[0]

                # 各馬のデータに race_id を追加してリストに格納
                for horse_result in single_race_data:
                    horse_result['raceId'] = race_id
                    all_horse_data.append(horse_result)

    except FileNotFoundError:
        print(f"[Error] Directory not found: {data_path}")
    except Exception as e:
        print(f"[Error] Failed to read or process data: {e}")

    print(f"✅ Successfully loaded data for {len(all_horse_data)} horses.")
    return all_horse_data
