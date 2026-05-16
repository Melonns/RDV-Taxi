"""
Kelompok Proyek Rekayasa Data:
1. 235150201111036 DARVESH AZIZ MAWLA
2. 235150207111063 ACHMAD ALVIAN PRASETIO
3. 235150207111006 DZAKY REZANDI
4. 235150201111004 WAHYU DWI LAKSANA PUTRI
5. 235150207111065 JONATHAN SALIM
"""

"""Export feature importances from trained ML models and analyze weather impact (Q3)."""

from pathlib import Path

import joblib
import pandas as pd


# Weather features that impact trip tip and duration
WEATHER_FEATURES = {
    "temperature_c": "Temperature (°C)",
    "precipitation_mm": "Precipitation (mm)",
    "wind_speed": "Wind Speed",
    "weathercode": "Weather Code",
}


def export_feature_importance(
    tip_model_path: str = "ml/saved/rf_tip.pkl",
    tip_features_file: str = "ml/saved/rf_tip_features.pkl",
    duration_model_path: str = "ml/saved/rf_duration.pkl",
    duration_features_file: str = "ml/saved/rf_duration_features.pkl",
    output_path: str = "data/intermediate/ml_results.parquet",
) -> dict:
    """Combine feature importances and analyze weather impact (Q3).
    
    Returns analysis summary for Q3: "How much impact does weather have?"
    """
    tip_model = joblib.load(tip_model_path)
    tip_features = joblib.load(tip_features_file)
    duration_model = joblib.load(duration_model_path)
    duration_features = joblib.load(duration_features_file)

    tip_importance = getattr(tip_model, "feature_importances_", None)
    duration_importance = getattr(duration_model, "feature_importances_", None)

    all_features = sorted(set(tip_features) | set(duration_features))
    rows = []
    for feature_name in all_features:
        tip_imp = (
            float(tip_importance[tip_features.index(feature_name)])
            if tip_importance is not None and feature_name in tip_features
            else 0.0
        )
        duration_imp = (
            float(duration_importance[duration_features.index(feature_name)])
            if duration_importance is not None and feature_name in duration_features
            else 0.0
        )
        row = {
            "feature_name": feature_name,
            "importance_tip": tip_imp,
            "importance_duration": duration_imp,
            "importance_avg": (tip_imp + duration_imp) / 2,
            "is_weather": feature_name in WEATHER_FEATURES,
        }
        rows.append(row)

    df = pd.DataFrame(rows).sort_values("importance_tip", ascending=False)

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_file, index=False)

    print(f"Saved ML results to {output_file}")

    # Analyze weather impact (Q3)
    analysis = analyze_weather_impact(df)

    return analysis


def analyze_weather_impact(features_df: pd.DataFrame) -> dict:
    """Analyze weather feature contributions (answers Q3).
    
    Q3: Seberapa besar pengaruh cuaca terhadap tip dan durasi trip?
    (How much impact does weather have on tip and trip duration?)
    """
    
    # Split weather vs non-weather features
    weather_df = features_df[features_df["is_weather"] == True].copy()
    non_weather_df = features_df[features_df["is_weather"] == False].copy()

    # Calculate total importance
    weather_tip_total = weather_df["importance_tip"].sum()
    weather_duration_total = weather_df["importance_duration"].sum()
    non_weather_tip_total = non_weather_df["importance_tip"].sum()
    non_weather_duration_total = non_weather_df["importance_duration"].sum()

    tip_total = weather_tip_total + non_weather_tip_total
    duration_total = weather_duration_total + non_weather_duration_total

    # Calculate percentages
    weather_tip_pct = (
        (weather_tip_total / tip_total * 100) if tip_total > 0 else 0
    )
    weather_duration_pct = (
        (weather_duration_total / duration_total * 100)
        if duration_total > 0
        else 0
    )

    # Top weather features
    top_weather = weather_df.nlargest(3, "importance_avg")[
        ["feature_name", "importance_tip", "importance_duration", "importance_avg"]
    ]

    analysis = {
        "weather_impact_tip_pct": round(weather_tip_pct, 2),
        "weather_impact_duration_pct": round(weather_duration_pct, 2),
        "weather_features_count": len(weather_df),
        "non_weather_features_count": len(non_weather_df),
        "top_weather_features": top_weather.to_dict(orient="records"),
        "weather_importance_total_tip": round(weather_tip_total, 6),
        "weather_importance_total_duration": round(weather_duration_total, 6),
        "non_weather_importance_total_tip": round(non_weather_tip_total, 6),
        "non_weather_importance_total_duration": round(
            non_weather_duration_total, 6
        ),
    }

    # Print summary (Q3 Answer)
    print("\n" + "=" * 70)
    print("Q3 ANALYSIS: Weather Impact on Trip Tip & Duration")
    print("=" * 70)
    print(f"\n📊 WEATHER FEATURE CONTRIBUTION (Feature Importance):\n")
    print(
        f"  Tip Prediction:      {weather_tip_pct:>6.2f}% dari total importance"
    )
    print(
        f"  Duration Prediction: {weather_duration_pct:>6.2f}% dari total importance"
    )
    print(
        f"  Average:             {(weather_tip_pct + weather_duration_pct) / 2:>6.2f}%\n"
    )

    print(f"📈 TOP WEATHER FEATURES:\n")
    for idx, row in enumerate(top_weather.to_dict(orient="records"), 1):
        feat_name = row["feature_name"]
        tip_imp = row["importance_tip"]
        dur_imp = row["importance_duration"]
        avg_imp = row["importance_avg"]
        print(
            f"  {idx}. {WEATHER_FEATURES.get(feat_name, feat_name):25} "
            f"(Tip: {tip_imp:.4f}, Duration: {dur_imp:.4f}, Avg: {avg_imp:.4f})"
        )

    print(f"\n💡 INTERPRETATION:\n")
    if weather_tip_pct > 15:
        print(f"  ✓ Cuaca memiliki PENGARUH SIGNIFIKAN pada tip ({weather_tip_pct:.1f}%)")
    else:
        print(
            f"  ✗ Cuaca memiliki pengaruh TERBATAS pada tip ({weather_tip_pct:.1f}%)"
        )

    if weather_duration_pct > 15:
        print(
            f"  ✓ Cuaca memiliki PENGARUH SIGNIFIKAN pada durasi ({weather_duration_pct:.1f}%)"
        )
    else:
        print(
            f"  ✗ Cuaca memiliki pengaruh TERBATAS pada durasi ({weather_duration_pct:.1f}%)"
        )

    print("\n" + "=" * 70 + "\n")

    return analysis


if __name__ == "__main__":
    result = export_feature_importance()
    print(f"\n✓ Analysis complete. Q3 Answer:")
    print(f"  - Weather impact on tip:      {result['weather_impact_tip_pct']}%")
    print(f"  - Weather impact on duration: {result['weather_impact_duration_pct']}%")


