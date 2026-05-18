"""
Kelompok Proyek Rekayasa Data:
1. 235150201111036 DARVESH AZIZ MAWLA
2. 235150207111063 ACHMAD ALVIAN PRASETIO
3. 235150207111006 DZAKY REZANDI
4. 235150201111004 WAHYU DWI LAKSANA PUTRI
5. 235150207111065 JONATHAN SALIM
"""

"""
Dashboard Streamlit untuk analisis NYC TLC Yellow Taxi.

Fokus dashboard:
- Ringkasan data perjalanan
- Peta zona pickup
- Analisis cuaca terhadap tip dan durasi perjalanan
- Feature importance dari model machine learning
"""

from pathlib import Path
import json

import duckdb
import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st


DB_PATH = Path("data/final/tlc.duckdb")
ML_PATH = Path("data/intermediate/ml_results.parquet")
ML_METRICS_PATH = Path("data/intermediate/ml_metrics.json")


# Palette sederhana: 2 warna utama + 1 accent + netral
COLOR_PRIMARY = "#2563EB"      # biru utama
COLOR_SECONDARY = "#64748B"    # abu-abu pembanding
COLOR_ACCENT = "#F59E0B"       # highlight
COLOR_NEGATIVE = "#DC2626"     # warning/risiko
COLOR_BACKGROUND = "#FFFFFF"
COLOR_GRID = "#E2E8F0"


@st.cache_data(show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    """Menjalankan query SQL ke DuckDB."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(query).fetchdf()
    finally:
        con.close()


@st.cache_data(show_spinner=False)
def get_columns(table_name: str) -> list[str]:
    """Mengambil daftar kolom dari tabel DuckDB."""
    df = run_query(f"DESCRIBE {table_name}")
    return df["column_name"].tolist()


def sql_list(values) -> str:
    """Mengubah list pilihan filter menjadi format SQL IN (...)."""
    if not values:
        return "''"

    escaped = [str(value).replace("'", "''") for value in values]
    return ", ".join(f"'{value}'" for value in escaped)


def style_figure(fig, showlegend: bool = False):
    """Merapikan tampilan chart agar konsisten."""
    fig.update_layout(
        showlegend=showlegend,
        plot_bgcolor=COLOR_BACKGROUND,
        paper_bgcolor=COLOR_BACKGROUND,
        margin=dict(l=20, r=20, t=60, b=30),
        font=dict(size=13, color="#1E293B"),
        xaxis=dict(gridcolor=COLOR_GRID),
        yaxis=dict(gridcolor=COLOR_GRID),
    )
    return fig


def build_metric_bar_chart(metric_name: str, duration_value: float, tip_value: float, title: str, color_duration: str, color_tip: str):
    """Membuat chart perbandingan dua model untuk satu metrik."""
    chart_df = pd.DataFrame(
        [
            {"model": "Durasi", "value": duration_value},
            {"model": "Tip", "value": tip_value},
        ]
    )

    fig = px.bar(
        chart_df,
        x="model",
        y="value",
        color="model",
        text_auto=".4f",
        title=title,
        labels={"model": "Model", "value": metric_name},
        color_discrete_map={"Durasi": color_duration, "Tip": color_tip},
    )
    fig.update_traces(textposition="outside")
    fig = style_figure(fig, showlegend=False)
    fig.update_yaxes(title_text=metric_name)
    return fig


@st.cache_data(show_spinner=False)
def load_ml_metrics() -> dict:
    """Membaca metrik evaluasi model dari file JSON."""
    if not ML_METRICS_PATH.exists():
        return {}

    with ML_METRICS_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def main():
    st.set_page_config(
        page_title="NYC TLC Weather Analysis",
        layout="wide",
    )

    st.title("Dashboard Analisis NYC Yellow Taxi")

    st.markdown(
        """
        Dashboard ini merangkum pola tip dan durasi perjalanan NYC Yellow Taxi
        berdasarkan cuaca, waktu, dan lokasi pickup pada periode Januari–Juni 2025.

        Data `tip_percentage` dibatasi pada rentang 0–100% untuk mengurangi pengaruh outlier ekstrem.
        """
    )

    if not DB_PATH.exists():
        st.error("Database final belum ditemukan: data/final/tlc.duckdb")
        st.stop()

    fact_columns = get_columns("fact_trips")
    location_columns = get_columns("dim_location")

    # Sidebar Filter

    st.sidebar.header("Filter")

    date_range = st.sidebar.date_input(
        "Rentang Tanggal",
        value=(pd.to_datetime("2025-01-01"), pd.to_datetime("2025-06-30")),
        min_value=pd.to_datetime("2025-01-01"),
        max_value=pd.to_datetime("2025-06-30"),
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date = date_range[0].strftime("%Y-%m-%d")
        end_date = (pd.to_datetime(date_range[1]) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        start_date = "2025-01-01"
        end_date = "2025-07-01"

    date_filter = f"""
    pickup_datetime >= DATE '{start_date}'
    AND pickup_datetime < DATE '{end_date}'
    AND tip_percentage BETWEEN 0 AND 100
    """

    date_filter_f = f"""
    f.pickup_datetime >= DATE '{start_date}'
    AND f.pickup_datetime < DATE '{end_date}'
    AND f.tip_percentage BETWEEN 0 AND 100
    """

    weather_options = run_query(f"""
        SELECT DISTINCT weather_description
        FROM fact_trips
        WHERE {date_filter}
          AND weather_description IS NOT NULL
        ORDER BY weather_description
    """)["weather_description"].tolist()

    selected_weather = st.sidebar.multiselect(
        "Kategori Cuaca",
        options=weather_options,
        default=weather_options,
    )

    temp_options = run_query(f"""
        SELECT DISTINCT temperature_category
        FROM fact_trips
        WHERE {date_filter}
          AND temperature_category IS NOT NULL
        ORDER BY temperature_category
    """)["temperature_category"].tolist()

    selected_temp = st.sidebar.multiselect(
        "Kategori Suhu",
        options=temp_options,
        default=temp_options,
    )

    if "borough" in location_columns:
        borough_options = run_query("""
            SELECT DISTINCT borough
            FROM dim_location
            WHERE borough IS NOT NULL
            ORDER BY borough
        """)["borough"].tolist()
    else:
        borough_options = []

    selected_borough = st.sidebar.multiselect(
        "Borough",
        options=borough_options,
        default=borough_options,
    )

    if "payment_type" in fact_columns:
        payment_options = run_query(f"""
            SELECT DISTINCT payment_type
            FROM fact_trips
            WHERE {date_filter}
              AND payment_type IS NOT NULL
            ORDER BY payment_type
        """)["payment_type"].tolist()

        selected_payment = st.sidebar.multiselect(
            "Payment Type",
            options=payment_options,
            default=payment_options,
        )
    else:
        selected_payment = []

    weather_filter = sql_list(selected_weather)
    temp_filter = sql_list(selected_temp)
    borough_filter = sql_list(selected_borough)

    base_filter = f"""
    {date_filter}
    AND weather_description IN ({weather_filter})
    AND temperature_category IN ({temp_filter})
    """

    base_filter_f = f"""
    {date_filter_f}
    AND f.weather_description IN ({weather_filter})
    AND f.temperature_category IN ({temp_filter})
    """

    if "borough" in location_columns and selected_borough:
        base_filter += f"""
        AND pickup_location_key IN (
            SELECT location_key
            FROM dim_location
            WHERE borough IN ({borough_filter})
        )
        """

        base_filter_f += f"""
        AND l.borough IN ({borough_filter})
        """

    if "payment_type" in fact_columns and selected_payment:
        payment_filter = ", ".join(str(int(value)) for value in selected_payment)

        base_filter += f"""
        AND payment_type IN ({payment_filter})
        """

        base_filter_f += f"""
        AND f.payment_type IN ({payment_filter})
        """

    st.sidebar.markdown("---")
    st.sidebar.caption("Data tip difilter pada rentang 0–100%.")

    tabs = st.tabs(
        [
            "Ringkasan",
            "Peta",
            "Cuaca vs Tip & Durasi",
            "ML Insights",
        ]
    )

    # Tab 0 Ringkasan

    with tabs[0]:
        st.subheader("Ringkasan Dataset")

        overview = run_query(f"""
            SELECT
                COUNT(*) AS total_trips,
                MIN(pickup_datetime) AS start_date,
                MAX(pickup_datetime) AS end_date,
                AVG(tip_percentage) AS avg_tip_percentage,
                AVG(trip_duration_min) AS avg_trip_duration_min,
                AVG(trip_distance) AS avg_trip_distance
            FROM fact_trips
            WHERE {base_filter}
        """)

        row = overview.iloc[0]

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Perjalanan", f"{row['total_trips']:,.0f}")
        col2.metric("Rata-rata Tip", f"{row['avg_tip_percentage']:.2f}%")
        col3.metric("Rata-rata Durasi", f"{row['avg_trip_duration_min']:.2f} menit")

        st.caption(f"Periode data aktif: {row['start_date']} sampai {row['end_date']}")

        weather_dist = run_query(f"""
            SELECT
                weather_description,
                COUNT(*) AS total_trips,
                AVG(tip_percentage) AS avg_tip_percentage,
                AVG(trip_duration_min) AS avg_trip_duration_min
            FROM fact_trips
            WHERE {base_filter}
            GROUP BY weather_description
            ORDER BY total_trips DESC
        """)

        fig = px.bar(
            weather_dist,
            x="weather_description",
            y="total_trips",
            text_auto=True,
            title="Distribusi Perjalanan per Kategori Cuaca",
            labels={
                "weather_description": "Kategori Cuaca",
                "total_trips": "Total Perjalanan",
            },
        )
        fig.update_traces(marker_color=COLOR_PRIMARY)
        fig = style_figure(fig)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("#### Ringkasan per Kategori Cuaca")
        st.dataframe(weather_dist, use_container_width=True)

    # Tab 1 Peta

    with tabs[1]:
        st.subheader("Peta Zona Pickup")

        st.markdown(
            """
            Peta menggunakan koordinat centroid dari `dim_location`.
            Karena data TLC Yellow Taxi 2025 berbasis `LocationID`, peta ini merepresentasikan
            agregasi per zona pickup, bukan titik GPS asli dari setiap perjalanan.
            """
        )

        map_df = run_query(f"""
            SELECT
                l.latitude,
                l.longitude,
                l.borough,
                l.zone_name,
                COUNT(*) AS total_trips,
                AVG(f.tip_percentage) AS avg_tip_percentage,
                AVG(f.trip_duration_min) AS avg_trip_duration_min
            FROM fact_trips f
            JOIN dim_location l
                ON f.pickup_location_key = l.location_key
            WHERE {base_filter_f}
              AND l.latitude IS NOT NULL
              AND l.longitude IS NOT NULL
              AND l.latitude BETWEEN 40.4 AND 41.0
              AND l.longitude BETWEEN -74.4 AND -73.5
            GROUP BY l.latitude, l.longitude, l.borough, l.zone_name
            ORDER BY total_trips DESC
        """)

        if map_df.empty:
            st.warning("Tidak ada koordinat valid untuk ditampilkan.")
        else:
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Zona Aktif", f"{len(map_df):,.0f}")
            col2.metric("Borough Aktif", f"{map_df['borough'].nunique():,.0f}")
            col3.metric("Zona Terpadat", str(map_df.iloc[0]["zone_name"]))

            st.pydeck_chart(
                pdk.Deck(
                    map_style=None,
                    initial_view_state=pdk.ViewState(
                        latitude=40.7128,
                        longitude=-74.0060,
                        zoom=10,
                        pitch=35,
                    ),
                    layers=[
                        pdk.Layer(
                            "HeatmapLayer",
                            data=map_df,
                            get_position="[longitude, latitude]",
                            get_weight="total_trips",
                            radius_pixels=45,
                            color_range=[
                                [245, 158, 11, 80],
                                [37, 99, 235, 180],
                            ],
                        )
                    ],
                )
            )

            borough_summary = run_query(f"""
                SELECT
                    l.borough,
                    COUNT(*) AS total_trips,
                    AVG(f.tip_percentage) AS avg_tip_percentage,
                    AVG(f.trip_duration_min) AS avg_trip_duration_min
                FROM fact_trips f
                JOIN dim_location l
                    ON f.pickup_location_key = l.location_key
                WHERE {base_filter_f}
                GROUP BY l.borough
                ORDER BY total_trips DESC
            """)

            fig = px.bar(
                borough_summary,
                x="borough",
                y="total_trips",
                text_auto=True,
                title="Volume Perjalanan per Borough",
                labels={
                    "borough": "Borough",
                    "total_trips": "Total Perjalanan",
                },
            )
            fig.update_traces(marker_color=COLOR_PRIMARY)
            fig = style_figure(fig)
            st.plotly_chart(fig, use_container_width=True)

            top_zones = run_query(f"""
                SELECT
                    l.borough,
                    l.zone_name,
                    COUNT(*) AS total_trips,
                    AVG(f.tip_percentage) AS avg_tip_percentage,
                    AVG(f.trip_duration_min) AS avg_trip_duration_min
                FROM fact_trips f
                JOIN dim_location l
                    ON f.pickup_location_key = l.location_key
                WHERE {base_filter_f}
                GROUP BY l.borough, l.zone_name
                ORDER BY total_trips DESC
                LIMIT 25
            """)

            st.markdown("#### Top 25 Zona Pickup Berdasarkan Volume Perjalanan")
            st.dataframe(top_zones, use_container_width=True)

    # Tab 2 Cuaca vs Tip & Durasi

    with tabs[2]:
        st.subheader("Cuaca vs Tip dan Durasi")

        weather_summary = run_query(f"""
            SELECT
                weather_description,
                COUNT(*) AS total_trips,
                AVG(tip_percentage) AS avg_tip_percentage,
                AVG(trip_duration_min) AS avg_trip_duration_min,
                AVG(trip_distance) AS avg_trip_distance,
                AVG(total_amount) AS avg_total_amount
            FROM fact_trips
            WHERE {base_filter}
            GROUP BY weather_description
        """)

        tip_weather = weather_summary.sort_values("avg_tip_percentage", ascending=False)
        duration_weather = weather_summary.sort_values("avg_trip_duration_min", ascending=False)

        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(
                tip_weather,
                x="weather_description",
                y="avg_tip_percentage",
                text_auto=".2f",
                title="Rata-rata Tip per Kategori Cuaca",
                labels={
                    "weather_description": "Kategori Cuaca",
                    "avg_tip_percentage": "Rata-rata Tip (%)",
                },
            )
            fig.update_traces(marker_color=COLOR_PRIMARY)
            fig = style_figure(fig)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                duration_weather,
                x="weather_description",
                y="avg_trip_duration_min",
                text_auto=".2f",
                title="Rata-rata Durasi per Kategori Cuaca",
                labels={
                    "weather_description": "Kategori Cuaca",
                    "avg_trip_duration_min": "Rata-rata Durasi (menit)",
                },
            )
            fig.update_traces(marker_color=COLOR_ACCENT)
            fig = style_figure(fig)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("#### Pola per Jam")

        metric_choice = st.radio(
            "Metrik tren per jam",
            ["Tip", "Durasi"],
            horizontal=True,
        )

        if metric_choice == "Tip":
            hourly_df = run_query(f"""
                SELECT
                    hour_of_day,
                    AVG(tip_percentage) AS metric_value
                FROM fact_trips
                WHERE {base_filter}
                GROUP BY hour_of_day
                ORDER BY hour_of_day
            """)

            y_label = "Rata-rata Tip (%)"
            title = "Tren Rata-rata Tip per Jam"
            line_color = COLOR_PRIMARY
        else:
            hourly_df = run_query(f"""
                SELECT
                    hour_of_day,
                    AVG(trip_duration_min) AS metric_value
                FROM fact_trips
                WHERE {base_filter}
                GROUP BY hour_of_day
                ORDER BY hour_of_day
            """)

            y_label = "Rata-rata Durasi (menit)"
            title = "Tren Rata-rata Durasi per Jam"
            line_color = COLOR_ACCENT

        fig = px.line(
            hourly_df,
            x="hour_of_day",
            y="metric_value",
            markers=True,
            title=title,
            labels={
                "hour_of_day": "Jam",
                "metric_value": y_label,
            },
        )
        fig.update_traces(line_color=line_color, marker_color=line_color)
        fig = style_figure(fig)
        st.plotly_chart(fig, use_container_width=True)

        peak_summary = run_query(f"""
            SELECT
                is_peak_hour,
                COUNT(*) AS total_trips,
                AVG(tip_percentage) AS avg_tip_percentage,
                AVG(trip_duration_min) AS avg_trip_duration_min
            FROM fact_trips
            WHERE {base_filter}
            GROUP BY is_peak_hour
            ORDER BY is_peak_hour
        """)

        peak_summary["periode"] = peak_summary["is_peak_hour"].map(
            {False: "Non-Peak Hour", True: "Peak Hour"}
        )

        fig = px.bar(
            peak_summary,
            x="periode",
            y="avg_trip_duration_min",
            text_auto=".2f",
            title="Durasi Rata-rata: Peak Hour dan Non-Peak Hour",
            labels={
                "periode": "Periode",
                "avg_trip_duration_min": "Rata-rata Durasi (menit)",
            },
        )
        fig.update_traces(marker_color=[COLOR_SECONDARY, COLOR_ACCENT])
        fig = style_figure(fig)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("#### Tabel Ringkasan Cuaca")
        st.dataframe(
            weather_summary.sort_values("avg_tip_percentage", ascending=False),
            use_container_width=True,
        )

    # Tab 3 ML Insights

    with tabs[3]:
        st.subheader("ML Insights")

        st.markdown(
            """
            Bagian ini menunjukkan fitur yang paling banyak dipakai model saat memprediksi
            persentase tip dan durasi perjalanan.
            """
        )

        metrics = load_ml_metrics()

        if metrics:
            st.markdown("#### Evaluasi Model")
            duration_metrics = metrics.get("duration_model", {})
            tip_metrics = metrics.get("tip_model", {})

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Model Durasi**")
                d1, d2, d3 = st.columns(3)
                d1.metric("CV RMSE", f"{duration_metrics.get('cv_rmse', float('nan')):.4f}")
                d2.metric("CV MAE", f"{duration_metrics.get('cv_mae', float('nan')):.4f}")
                d3.metric("CV R²", f"{duration_metrics.get('cv_r2', float('nan')):.4f}")

            with col2:
                st.markdown("**Model Tip**")
                t1, t2, t3 = st.columns(3)
                t1.metric("CV RMSE", f"{tip_metrics.get('cv_rmse', float('nan')):.4f}")
                t2.metric("CV MAE", f"{tip_metrics.get('cv_mae', float('nan')):.4f}")
                t3.metric("CV R²", f"{tip_metrics.get('cv_r2', float('nan')):.4f}")

            st.markdown("#### Grafik Perbandingan")
            c1, c2, c3 = st.columns(3)

            with c1:
                st.plotly_chart(
                    build_metric_bar_chart(
                        "CV RMSE",
                        duration_metrics.get("cv_rmse", 0.0),
                        tip_metrics.get("cv_rmse", 0.0),
                        "Perbandingan CV RMSE",
                        COLOR_PRIMARY,
                        COLOR_ACCENT,
                    ),
                    use_container_width=True,
                )

            with c2:
                st.plotly_chart(
                    build_metric_bar_chart(
                        "CV MAE",
                        duration_metrics.get("cv_mae", 0.0),
                        tip_metrics.get("cv_mae", 0.0),
                        "Perbandingan CV MAE",
                        COLOR_PRIMARY,
                        COLOR_ACCENT,
                    ),
                    use_container_width=True,
                )

            with c3:
                st.plotly_chart(
                    build_metric_bar_chart(
                        "CV R²",
                        duration_metrics.get("cv_r2", 0.0),
                        tip_metrics.get("cv_r2", 0.0),
                        "Perbandingan CV R²",
                        COLOR_PRIMARY,
                        COLOR_ACCENT,
                    ),
                    use_container_width=True,
                )

            with st.expander("Lihat detail angka evaluasi"):
                metrics_df = pd.DataFrame(
                    [
                        {"model": "duration", **duration_metrics},
                        {"model": "tip", **tip_metrics},
                    ]
                )
                st.dataframe(metrics_df, use_container_width=True)

        if not ML_PATH.exists():
            st.warning("File ML results belum tersedia: data/intermediate/ml_results.parquet")
        else:
            ml_df = pd.read_parquet(ML_PATH)

            if "is_weather" in ml_df.columns:
                weather_features = ml_df[ml_df["is_weather"] == True]
            else:
                weather_features = pd.DataFrame()

            if not weather_features.empty:
                tip_weather_contribution = weather_features["importance_tip"].sum()
                duration_weather_contribution = weather_features["importance_duration"].sum()

                col1, col2 = st.columns(2)
                col1.metric(
                    "Kontribusi Cuaca terhadap Tip",
                    f"{tip_weather_contribution * 100:.2f}%",
                )
                col2.metric(
                    "Kontribusi Cuaca terhadap Durasi",
                    f"{duration_weather_contribution * 100:.2f}%",
                )

            col1, col2 = st.columns(2)

            with col1:
                if "importance_tip" in ml_df.columns and "feature_name" in ml_df.columns:
                    tip_importance = ml_df.sort_values("importance_tip", ascending=False)

                    fig = px.bar(
                        tip_importance,
                        x="feature_name",
                        y="importance_tip",
                        color="is_weather" if "is_weather" in tip_importance.columns else None,
                        title="Feature Importance untuk Tip",
                        labels={
                            "feature_name": "Fitur",
                            "importance_tip": "Importance",
                            "is_weather": "Fitur Cuaca",
                        },
                        color_discrete_map={
                            True: COLOR_ACCENT,
                            False: COLOR_PRIMARY,
                        },
                    )
                    fig = style_figure(fig, showlegend=True)
                    st.plotly_chart(fig, use_container_width=True)

            with col2:
                if "importance_duration" in ml_df.columns and "feature_name" in ml_df.columns:
                    duration_importance = ml_df.sort_values("importance_duration", ascending=False)

                    fig = px.bar(
                        duration_importance,
                        x="feature_name",
                        y="importance_duration",
                        color="is_weather" if "is_weather" in duration_importance.columns else None,
                        title="Feature Importance untuk Durasi",
                        labels={
                            "feature_name": "Fitur",
                            "importance_duration": "Importance",
                            "is_weather": "Fitur Cuaca",
                        },
                        color_discrete_map={
                            True: COLOR_ACCENT,
                            False: COLOR_PRIMARY,
                        },
                    )
                    fig = style_figure(fig, showlegend=True)
                    st.plotly_chart(fig, use_container_width=True)

            st.markdown("#### Detail Feature Importance")
            st.dataframe(ml_df, use_container_width=True)


if __name__ == "__main__":
    main()

