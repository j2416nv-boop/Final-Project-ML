# Three-Way Equity Classifier Comparison

**Group 8 | University of South Florida | ML Creative Project 3**

This project compares three S&P 500 classification schemes head-to-head:
GICS sectors, SIC-based FF12 industries, and K-Means clusters built on
return co-movement. Each scheme is fed through the same pipeline —
composition, homogeneity, alpha prediction, anomaly detection — and
results are compared on the 2023–2024 holdout.

## Project structure

```
.
├── report.qmd               # main Quarto report — RUN THIS
├── R/
│   ├── utils.R              # paths, parquet caching, math helpers
│   ├── 01_pull_crsp.R       # WRDS: members, returns, SIC, GICS
│   ├── 02_pull_french.R     # Ken French: FF5, FF12 industry defs
│   ├── 03_pull_fred.R       # FRED: 10Y, FFR, VIX
│   ├── 04_build_panel.R     # master monthly panel
│   ├── 05_features.R        # rolling betas, alpha, controls (with FF lag)
│   ├── 06_clustering.R      # annual K-Means on return co-movement
│   └── 07_compare.R         # the three-way comparison battery
├── data/
│   ├── raw/                 # parquet caches of pulled raw data
│   └── processed/           # panel.parquet, features.parquet, clusters_*
└── figures/                 # any figures saved outside the QMD
```

## Setup

### R packages

Run once:

```r
install.packages(c(
  "dplyr", "tidyr", "purrr", "tibble", "stringr", "readr", "lubridate",
  "arrow",                              # parquet I/O
  "ggplot2", "knitr", "scales",         # report
  "DBI", "RPostgres",                   # WRDS
  "glmnet", "isotree",                  # ML
  "cluster", "clue", "mclust",          # clustering helpers + ARI
  "rmarkdown", "quarto"
))
```

### WRDS credentials

The CRSP pulls require a WRDS account. Set these once in your R session
before knitting:

```r
Sys.setenv(WRDS_USER = "N/A")
Sys.setenv(WRDS_PASSWORD = "N/A")
```

Or — preferred — add a line to your `~/.pgpass`:

```
wrds-pgdata.wharton.upenn.edu:9737:wrds:YOUR_USERNAME:YOUR_PASSWORD
```

On Windows, use `%APPDATA%/postgresql/pgpass.conf` instead of `~/.pgpass`.
On Unix-like systems, lock down permissions with `chmod 600 ~/.pgpass`.

### Network access

The pipeline downloads from three external hosts. Make sure these are
reachable on the machine that runs the report:

- `wrds-pgdata.wharton.upenn.edu` (CRSP)
- `mba.tuck.dartmouth.edu` (Ken French)
- `fred.stlouisfed.org` (FRED, public CSVs, no key)

## Running

### Dev mode (default — 50 stocks, ~5–10 min on a laptop)

```bash
quarto render report.qmd --to html
```

`CFG$dev_mode = TRUE` at the top of `report.qmd` subsets to a random sample
of 50 permnos. Use this for iteration; numbers are not reportable.

### Full mode (all S&P 500 constituents 2014–2024)

Edit `report.qmd`:

```r
CFG$dev_mode <- FALSE
```

Then re-render. Expect 30–60 minutes the first time (data pulls cache to
`data/raw/*.parquet`; subsequent runs are fast).

### Re-pulling raw data

Caches in `data/raw/` are reused across runs. To force a re-pull, delete
the relevant parquet file (e.g. `rm data/raw/crsp_msf.parquet`) and
re-render.

## Design notes (responses to professor feedback)

1. **FF publication lag.** Ken French publishes FF5 with a multi-month
   delay. Any feature derived from FF5 *returns* is lagged by
   `CFG$ff_lag_months` (= 2) so the model would be deployable in real
   time. Realised alpha at month $t$ is computed using contemporaneous
   $F_t$ — but it is a *label* (the target), never a feature.

2. **Target = alpha, not return.** Predicting raw return from factor
   exposures would mostly recover the standard risk premia (high beta →
   high return). The economically meaningful target is the residual
   after factor exposure is priced out, i.e. realised alpha.

3. **Three-way industry-classifier comparison.** Comparing GICS to FF5
   factor loadings is incommensurable — one is an industry taxonomy, the
   other a risk taxonomy. We compare GICS to two other industry
   taxonomies: SIC/FF12 (the academic-finance counterpart) and K-Means
   on return co-movement (an unsupervised industry-style partition).
   K-Means features are the cross-sectional correlation columns, *not*
   FF5 loadings.

## Reproducibility

- Random seed: `CFG$seed = 8675309`. K-Means uses `nstart = 25`–`50` for
  stability.
- All raw pulls cache to parquet; feature engineering is deterministic
  given cached inputs.
- Anti-leakage tests run inside the report (Section 4.2). The render
  fails if any test fails.

## Known limitations

- Dev-mode 50-stock samples are for iteration only; final numbers must
  come from full mode.
- The K-Means cluster stability check is informal (we report chosen $k$
  by refit year). A more rigorous bootstrap-stability analysis would
  strengthen Section 5.
- Alpha is FF5-residual; momentum, quality, and STR factors are not
  netted out. An alpha-extension robustness check is a natural next step.
