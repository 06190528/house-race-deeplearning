from pathlib import Path

# このファイル (config.py) があるディレクトリの絶対パスを取得
# 例: /Users/username/project/analytical_ai/config
CONFIG_DIR = Path(__file__).parent.resolve()

# CONFIG_DIRから2つ上の階層に上がり、目的のフォルダへのパスを構築
# .resolve() を使うことで、'../' などを解決した綺麗な絶対パスになる
DATA_PATH = (CONFIG_DIR / '../../functions/scrape/racedata').resolve()

# -------------------------------------------------------------
# 使い方:
# 別のPythonファイルから以下のようにインポートして使用します。
# from config import DATA_PATH
#
# print(DATA_PATH)
# -------------------------------------------------------------