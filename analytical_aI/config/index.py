from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.resolve()
CONFIG_DIR = Path(__file__).parent.resolve()

DATA_PATH = (CONFIG_DIR / '../../functions/scrape/racedata').resolve()
MODELS_DIR = (PROJECT_ROOT / 'models').resolve()

# 全データをrace_id昇順でソートしたときの学習用比率（残りはバックテスト用）
TRAIN_RATIO = 0.8