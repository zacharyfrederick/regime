# Validation scripts

Optional spot-check scripts you can run after specific pipeline steps. The full validation is in `08_validation.py` and `notebooks/01_validate_pipeline.ipynb`; these snippets are for ad-hoc checks.

## Price features (after 03_price_features.py)

Assumes `config` is on the path. Run on debug tickers and check the assertions; if they pass, 03_price_features is complete.

```python
import pandas as pd
from config import DATE_START, DATE_END

price = pd.read_parquet("outputs/features/price_features.parquet")

# Basic sanity
assert price[["ticker", "date"]].duplicated().sum() == 0, "Duplicate ticker-dates"
assert (price["date"] >= pd.Timestamp(DATE_START)).all(), "Dates before range"
assert (price["date"] <= pd.Timestamp(DATE_END)).all(), "Dates after range"

# Vol spike in March 2020
vol_march = price[price["date"].between("2020-03-01", "2020-03-31")]["vol_20d"].mean()
vol_2019  = price[price["date"].between("2019-01-01", "2019-12-31")]["vol_20d"].mean()
assert vol_march > vol_2019 * 2, f"Vol should spike in March 2020: {vol_march:.3f} vs {vol_2019:.3f}"

# HTZ near 52w low before bankruptcy
htz = price[(price["ticker"] == "HTZ") & (price["date"].between("2020-04-01", "2020-05-15"))]
assert (htz["pct_52w_range"].dropna() < 0.2).any(), "HTZ should be near 52w low"

# ret_12m null before sufficient history (first 252 trading days)
early = price[price["ticker"] == "AAPL"].sort_values("date").head(252)
assert early["ret_12m"].isna().all(), "ret_12m should be null before 252 days"

# ATR is dimensionless (fraction of price, not dollars)
atr_median = price["atr_14d"].dropna().median()
assert atr_median < 0.10, f"ATR should be fraction of price ~0.01-0.05, got {atr_median:.3f}"

# vol_20d is annualized — should be in range 0.10-0.80 for most stocks
vol_median = price["vol_20d"].dropna().median()
assert 0.10 < vol_median < 0.80, f"Annualized vol should be 10-80%, got {vol_median:.3f}"

print("Price features validation passed")
```

## Sector-relative (after 05_sector_relative.py)

The pipeline script runs these checks internally; you can re-run them in a notebook:

```python
import pandas as pd
sector = pd.read_parquet("outputs/features/sector_relative.parquet")
assert abs(sector["pe_vs_sector"].dropna().median() - 1.0) < 0.3, "pe_vs_sector median should be near 1.0"
assert abs(sector["roic_vs_sector"].dropna().mean()) < 0.05, "roic_vs_sector should average near 0"
universe = pd.read_parquet("outputs/universe/daily_universe.parquet")
has_sector = universe[universe["famaindustry"].notna()]
merged = has_sector.merge(sector, on=["ticker", "date"])
print("pe_vs_sector null rate for tickers with sector:", merged["pe_vs_sector"].isna().mean())
```
