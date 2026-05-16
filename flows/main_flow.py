"""
Kelompok Proyek Rekayasa Data:
1. 235150201111036 DARVESH AZIZ MAWLA
2. 235150207111063 ACHMAD ALVIAN PRASETIO
3. 235150207111006 DZAKY REZANDI
4. 235150201111004 WAHYU DWI LAKSANA PUTRI
5. 235150207111065 JONATHAN SALIM
"""

"""Legacy compatibility wrapper for the main pipeline."""

from pipeline.prefect_flow import main_pipeline as main_elt_pipeline


__all__ = ["main_elt_pipeline"]


if __name__ == "__main__":
    main_elt_pipeline()


