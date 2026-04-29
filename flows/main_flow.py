"""Legacy compatibility wrapper for the main pipeline."""

from pipeline.prefect_flow import main_pipeline as main_elt_pipeline


__all__ = ["main_elt_pipeline"]


if __name__ == "__main__":
    main_elt_pipeline()
