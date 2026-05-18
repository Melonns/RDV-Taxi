"""
Kelompok Proyek Rekayasa Data:
1. 235150201111036 DARVESH AZIZ MAWLA
2. 235150207111063 ACHMAD ALVIAN PRASETIO
3. 235150207111006 DZAKY REZANDI
4. 235150201111004 WAHYU DWI LAKSANA PUTRI
5. 235150207111065 JONATHAN SALIM
"""

"""Train a Random Forest model to predict trip duration in minutes."""

from pathlib import Path
import math
import logging

import joblib
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import cross_validate, train_test_split

logger = logging.getLogger(__name__)

MAX_TRAIN_ROWS = 150_000
N_ESTIMATORS = 200
MAX_DEPTH = 20


def train_duration_model(
    features_path: str = "data/intermediate/ml_features.parquet",
    save_dir: str = "ml/saved",
) -> dict:
    """Train, evaluate, and save the duration model."""
    logger.info(f"[DURATION] Loading feature matrix from {features_path}")
    df = pd.read_parquet(features_path)
    logger.info(f"[DURATION] Loaded {len(df):,} rows, {len(df.columns)} columns")

    target = "trip_duration_min"
    if target not in df.columns:
        raise RuntimeError("trip_duration_min target not found in feature matrix")

    # Data validation & cleaning
    logger.info(f"[DURATION] Validating feature matrix...")
    
    # Drop columns with ALL NaN values
    cols_before = len(df.columns)
    df = df.dropna(axis=1, how='all')
    logger.info(f"[DURATION] Dropped {cols_before - len(df.columns)} columns with all NaN")
    
    # Fill remaining NaN with median (numeric) or drop rows with NaN in target
    if df[target].isnull().any():
        logger.warning(f"[DURATION] Found {df[target].isnull().sum()} NaN in target column, dropping rows")
        df = df[df[target].notna()].copy()
    
    # Fill numeric NaN with column median
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].isnull().any():
            fill_value = df[col].median()
            logger.warning(f"[DURATION] Filling {df[col].isnull().sum()} NaN in {col} with median {fill_value:.4f}")
            df[col].fillna(fill_value, inplace=True)
    
    logger.info(f"[DURATION] After cleaning: {len(df):,} rows, {len(df.columns)} columns")

    if len(df) > MAX_TRAIN_ROWS:
        logger.info(f"[DURATION] Sampling {MAX_TRAIN_ROWS:,} rows from {len(df):,} for faster training")
        df = df.sample(n=MAX_TRAIN_ROWS, random_state=42).reset_index(drop=True)
        logger.info(f"[DURATION] After sampling: {len(df):,} rows")

    X = df.drop(columns=[target, "tip_percentage"], errors="ignore")
    y = df[target].values
    logger.info(f"[DURATION] Features shape: {X.shape}, Target shape: {y.shape}")

    logger.info(f"[DURATION] Splitting data (80/20)...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    logger.info(f"[DURATION] Train: {len(X_train):,}, Test: {len(X_test):,}")

    # Initialize model
    logger.info(
        f"[DURATION] Initializing RandomForestRegressor (n_estimators={N_ESTIMATORS}, max_depth={MAX_DEPTH})..."
    )
    model = RandomForestRegressor(
        n_estimators=N_ESTIMATORS,
        max_depth=MAX_DEPTH,
        random_state=42,
        n_jobs=1,
        verbose=1,
    )

    # Training
    logger.info(f"[DURATION] Training model... (this may take 1-5 minutes)")
    model.fit(X_train, y_train)
    logger.info(f"[DURATION] Model training complete!")

    # Cross-validation
    logger.info(f"[DURATION] Computing cross-validation scores (cv=5)...")
    cv_scores = cross_validate(
        model,
        X_train,
        y_train,
        cv=5,
        scoring={
            "rmse": "neg_root_mean_squared_error",
            "mae": "neg_mean_absolute_error",
            "r2": "r2",
        },
        n_jobs=1,
        return_train_score=False,
    )
    cv_rmse = float(-cv_scores["test_rmse"].mean())
    cv_mae = float(-cv_scores["test_mae"].mean())
    cv_r2 = float(cv_scores["test_r2"].mean())
    logger.info(
        f"[DURATION] CV RMSE: {cv_rmse:.4f}, CV MAE: {cv_mae:.4f}, CV R²: {cv_r2:.4f}"
    )

    # Evaluation
    logger.info(f"[DURATION] Evaluating on test set...")
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    train_rmse = float(math.sqrt(mean_squared_error(y_train, y_pred_train)))
    test_rmse = float(math.sqrt(mean_squared_error(y_test, y_pred_test)))
    test_r2 = float(r2_score(y_test, y_pred_test))
    logger.info(f"[DURATION] Train RMSE: {train_rmse:.4f}, Test RMSE: {test_rmse:.4f}, Test R²: {test_r2:.4f}")

    # Save model
    logger.info(f"[DURATION] Saving model to {save_dir}...")
    save_path = Path(save_dir)
    save_path.mkdir(parents=True, exist_ok=True)
    model_path = save_path / "rf_duration.pkl"
    features_file = save_path / "rf_duration_features.pkl"
    joblib.dump(model, model_path)
    joblib.dump(list(X.columns), features_file)
    logger.info(f"[DURATION] Model saved to {model_path}")

    result = {
        "model_path": str(model_path),
        "features_file": str(features_file),
        "cv_rmse": cv_rmse,
        "cv_mae": cv_mae,
        "cv_r2": cv_r2,
        "train_rmse": train_rmse,
        "test_rmse": test_rmse,
        "test_r2": test_r2,
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
    }

    logger.info(f"[DURATION] Duration model training complete: {result}")
    return result


if __name__ == "__main__":
    train_duration_model()


