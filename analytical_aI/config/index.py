from pathlib import Path

# このファイル(config.py)の親ディレクトリ(analytical_ai)を
# このPythonプロジェクトの基準点（ルート）とする
PROJECT_ROOT = Path(__file__).parent.resolve()

# このファイル (config.py) があるディレクトリの絶対パスを取得
# 例: /Users/username/project/analytical_ai/config
CONFIG_DIR = Path(__file__).parent.resolve()

# CONFIG_DIRから2つ上の階層に上がり、目的のフォルダへのパスを構築
# .resolve() を使うことで、'../' などを解決した綺麗な絶対パスになる
DATA_PATH = (CONFIG_DIR / '../../functions/scrape/racedata').resolve()

# 2. 未知データ(評価用)を保存するフォルダ
UNTOUCHED_DATA_DIR = (PROJECT_ROOT / 'untouched_data').resolve()

# 3. 学習済みモデルを保存するフォルダ
MODELS_DIR = (PROJECT_ROOT / 'models').resolve()

# 4. 学習用データを保存するフォルダ
TRAINING_DATA_DIR = (PROJECT_ROOT / 'training_data').resolve()
# -------------------------------------------------------------
# 使い方:
# 別のPythonファイルから以下のようにインポートして使用します。
# from config import DATA_PATH
#
# print(DATA_PATH)
# -------------------------------------------------------------