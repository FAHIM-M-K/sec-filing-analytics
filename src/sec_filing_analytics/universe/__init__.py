"""Loading and validating the project's fixed S&P 500 company roster."""

from .membership import load_membership_csv

__all__ = ["load_membership_csv"]
