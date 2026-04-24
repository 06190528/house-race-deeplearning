import sys
import os
import argparse
import numpy as np
import pandas as pd
import lightgbm as lgb
from lightgbm.callback import early_stopping, log_evaluation
from concurrent.futures import ThreadPoolExecutor, as_completed
import optuna

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

from analytical_aI.config.index import DATA_PATH, TRAIN_RATIO
from analytical_aI.data.loader import load_and_split_data
from analytical_aI.data.preprocessor import FEATURE_COLS, CAT_COLS

optuna.logging.set_verbosity(optuna.logging.WARNING)


def softmax(x):
    e_x = np.exp(x - np.max(x))
    return e_x / e_x.sum()


def calculate_roi(df, bet_threshold, win_rate_threshold, race_budget=100):
    mask = (df['expected_value'] > bet_threshold) & (df['predicted_win_rate'] > win_rate_threshold)
    value_bets = df[mask].copy()
    if value_bets.empty:
        return 0.0, 0
    prob_sums = value_bets.groupby('race_id')['predicted_win_rate'].transform('sum')
    value_bets['bet_amount'] = race_budget * (value_bets['predicted_win_rate'] / prob_sums)
    value_bets['return'] = np.where(
        value_bets['label'] == 3,
        value_bets['bet_amount'] * value_bets['odds'],
        0.0
    )
    bet_races = value_bets['race_id'].nunique()
    total_investment = bet_races * race_budget
    total_return = value_bets['return'].sum()
    roi = (total_return / total_investment) * 100 if total_investment > 0 else 0.0
    return roi, bet_races


def run_trial(trial_idx, train_df, unseen_df, available_features, seed):
    # --- 学習/検証分割 ---
    unique_races = sorted(train_df['race_id'].unique())
    split_idx = int(len(unique_races) * 0.8)
    train_races = set(unique_races[:split_idx])
    val_races   = set(unique_races[split_idx:])

    t_df = train_df[train_df['race_id'].isin(train_races)].copy()
    v_df = train_df[train_df['race_id'].isin(val_races)].copy()

    X_train = t_df[available_features]
    y_train = t_df['label']
    group_train = t_df.groupby('race_id', sort=False).size().tolist()
    X_val   = v_df[available_features]
    y_val   = v_df['label']
    group_val = v_df.groupby('race_id', sort=False).size().tolist()

    # --- LambdaRank ---
    model = lgb.LGBMRanker(
        objective="lambdarank", metric="ndcg", boosting_type="gbdt",
        n_estimators=1000, learning_rate=0.05, num_leaves=63,
        importance_type="gain", random_state=seed, n_jobs=1,
    )
    model.fit(
        X_train, y_train, group=group_train,
        eval_set=[(X_val, y_val)], eval_group=[group_val], eval_at=[3, 5],
        categorical_feature=CAT_COLS,
        callbacks=[early_stopping(stopping_rounds=50), log_evaluation(period=-1)],
    )

    # --- 未知データで予測 ---
    df = unseen_df.copy()
    df['predicted_score'] = model.predict(df[available_features])
    df['predicted_win_rate'] = df.groupby('race_id')['predicted_score'].transform(
        lambda x: softmax(x.values)
    )
    df['expected_value'] = df['predicted_win_rate'] * df['odds']

    # 前半でOptuna最適化、後半でTrue ROI測定
    all_races  = sorted(df['race_id'].unique())
    split      = len(all_races) // 2
    df_optuna  = df[df['race_id'].isin(all_races[:split])]
    df_test    = df[df['race_id'].isin(all_races[split:])]
    total_races = df['race_id'].nunique()

    sampler = optuna.samplers.TPESampler(seed=seed)
    study   = optuna.create_study(direction="maximize", sampler=sampler)

    def objective(trial):
        bet_threshold      = trial.suggest_float("bet_threshold", 1.1, 2.0)
        win_rate_threshold = trial.suggest_float("win_rate_threshold", 0.07, 0.20)
        roi, bet_races = calculate_roi(df_optuna, bet_threshold, win_rate_threshold)
        if bet_races < total_races * 0.25:
            return 0.0
        return roi

    study.optimize(objective, n_trials=500)

    roi, bet_races = calculate_roi(
        df_test,
        study.best_params['bet_threshold'],
        study.best_params['win_rate_threshold'],
    )
    test_races = df_test['race_id'].nunique()
    participation = bet_races / test_races * 100 if test_races > 0 else 0.0

    return {
        'trial': trial_idx + 1,
        'seed':  seed,
        'roi':   roi,
        'participation': participation,
        'best_params': study.best_params,
        'importances': model.feature_importances_.tolist(),
    }


def main(n_trials=20, base_seed=42):
    print("データを読み込み中（1回のみ）...")
    train_df, unseen_df = load_and_split_data(DATA_PATH, TRAIN_RATIO)

    available_features = [f for f in FEATURE_COLS if f in train_df.columns]
    print(f"学習用: {train_df['race_id'].nunique()} レース / 未知データ: {unseen_df['race_id'].nunique()} レース")
    print(f"試行回数: {n_trials} (並列実行)\n")

    results = [None] * n_trials
    seeds   = [base_seed + i for i in range(n_trials)]

    with ThreadPoolExecutor(max_workers=n_trials) as executor:
        futures = {
            executor.submit(run_trial, i, train_df, unseen_df, available_features, seeds[i]): i
            for i in range(n_trials)
        }
        for future in as_completed(futures):
            r = future.result()
            results[r['trial'] - 1] = r
            print(f"[Trial {r['trial']}/{n_trials}] seed={r['seed']}  ROI={r['roi']:.2f}%  参加率={r['participation']:.1f}%  params={r['best_params']}")

    rois = [r['roi'] for r in results]
    print(f"\n{'='*55}")
    print(f"平均ROI : {np.mean(rois):.2f}% ± {np.std(rois):.2f}%")
    print(f"最小/最大: {np.min(rois):.2f}% / {np.max(rois):.2f}%")
    print(f"{'='*55}")

    # --- 特徴量重要度（全 trial の Gain 平均） ---
    print("\n--- 特徴量重要度（Gain 平均・上位20件）---")
    mean_imp = np.mean([r['importances'] for r in results], axis=0)
    df_imp = pd.DataFrame({'feature': available_features, 'importance': mean_imp})
    df_imp = df_imp.sort_values('importance', ascending=False).reset_index(drop=True)
    print(df_imp.to_string())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-trials", type=int, default=20, help="試行回数（デフォルト: 10）")
    parser.add_argument("--base-seed", type=int, default=42, help="ベースシード（デフォルト: 42）")
    args = parser.parse_args()
    main(args.n_trials, args.base_seed)
