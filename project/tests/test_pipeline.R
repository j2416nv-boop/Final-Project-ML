# ============================================================================
# tests/test_pipeline.R
# Exercise the pipeline on synthetic data — no WRDS, no Ken French, no FRED.
# Goal: catch logic errors in features, clustering, comparison code before
# the real pipeline runs.
# ============================================================================

# Stub arrow I/O wrappers (arrow not installed in this sandbox).
write_parquet_safe <- function(x, path) invisible(NULL)
read_parquet_safe  <- function(path) stop("read_parquet_safe not used in tests")
assign("write_parquet_safe", write_parquet_safe, envir = .GlobalEnv)
assign("read_parquet_safe",  read_parquet_safe,  envir = .GlobalEnv)

# Stub out isotree for the anomaly test (not installed). We replace it
# with a trivial scorer based on Mahalanobis distance.
isotree <- list(
  isolation.forest = function(X, ...) {
    list(mu = colMeans(X), S = stats::cov(X))
  }
)
predict.isotree_stub <- function(object, newdata, ...) {
  d <- mahalanobis(newdata, object$mu, object$S, tol = 1e-12)
  d
}
# Make `predict` dispatch on our stub object.
assign("isotree", isotree, envir = .GlobalEnv)

# Patch the `predict()` calls inside compare.R won't dispatch on a list;
# we'll handle this by overriding predict for the test.

# Bootstrap utils manually (skip the library calls that aren't available).
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(purrr)
  library(tibble)
  library(stringr)
  library(readr)
})

PATHS <- list(raw = "test_data/raw", processed = "test_data/processed",
              figures = "test_data/figures")
for (p in PATHS) dir.create(p, recursive = TRUE, showWarnings = FALSE)

log_msg <- function(...) cat("[test]", ..., "\n", sep = "")

cache_parquet <- function(name, fn, force = FALSE, dir = PATHS$raw) {
  fn()  # never cache in tests
}

eom <- function(d) lubridate::ceiling_date(as.Date(d), "month") - 1

roll_ff5_betas <- function(y, X, min_obs = 24) {
  ok <- complete.cases(y, X)
  if (sum(ok) < min_obs) {
    return(c(alpha = NA_real_, beta_mkt = NA_real_, beta_smb = NA_real_,
             beta_hml = NA_real_, beta_rmw = NA_real_, beta_cma = NA_real_,
             rsq = NA_real_, n_obs = sum(ok)))
  }
  y <- y[ok]; X <- X[ok, , drop = FALSE]
  fit <- stats::lm.fit(cbind(1, as.matrix(X)), y)
  coefs <- fit$coefficients; yhat <- fit$fitted.values
  ss_res <- sum((y - yhat)^2); ss_tot <- sum((y - mean(y))^2)
  rsq <- if (ss_tot > 0) 1 - ss_res / ss_tot else NA_real_
  c(alpha=unname(coefs[1]), beta_mkt=unname(coefs[2]),
    beta_smb=unname(coefs[3]), beta_hml=unname(coefs[4]),
    beta_rmw=unname(coefs[5]), beta_cma=unname(coefs[6]),
    rsq=rsq, n_obs=sum(ok))
}

cs_zscore <- function(x) {
  mu <- mean(x, na.rm=TRUE); sd <- stats::sd(x, na.rm=TRUE)
  if (!is.finite(sd) || sd == 0) return(rep(0, length(x)))
  (x - mu) / sd
}

set_seed <- function(seed = 8675309) { set.seed(seed); invisible(seed) }
`%||%` <- function(a,b) if (is.null(a) || (length(a)==1 && is.na(a))) b else a

# ---- Build synthetic data ---------------------------------------------------
# 60 stocks, 132 months (2014-01 to 2024-12), three "true" sectors of 20
# stocks each, distinct factor exposures and idiosyncratic vol.

set.seed(42)
n_stocks <- 60
months <- unique(eom(seq(as.Date("2014-01-01"), as.Date("2024-12-31"), by = "month")))

# Three latent groups with distinct mean betas and idio vol.
group_id <- rep(1:3, length.out = n_stocks)
beta_mkt_true <- c(0.8, 1.2, 1.0)[group_id] + rnorm(n_stocks, sd = 0.15)
beta_smb_true <- c(-0.2, 0.5, 0.0)[group_id] + rnorm(n_stocks, sd = 0.20)
beta_hml_true <- c(0.4, -0.3, 0.0)[group_id] + rnorm(n_stocks, sd = 0.20)
beta_rmw_true <- rnorm(n_stocks, sd = 0.25)
beta_cma_true <- rnorm(n_stocks, sd = 0.25)

# Factor returns
fac <- tibble(
  date_eom = months,
  mkt_rf = rnorm(length(months), 0.005, 0.045),
  smb    = rnorm(length(months), 0.001, 0.025),
  hml    = rnorm(length(months), 0.000, 0.025),
  rmw    = rnorm(length(months), 0.002, 0.020),
  cma    = rnorm(length(months), 0.001, 0.020),
  rf     = rnorm(length(months), 0.001, 0.001)
)

# Stock excess returns
panel <- expand.grid(permno = 10001:(10000 + n_stocks),
                     date_eom = months) |>
  as_tibble() |>
  arrange(permno, date_eom)

panel$idx <- panel$permno - 10000
panel <- panel |>
  left_join(fac, by = "date_eom") |>
  mutate(
    bm = beta_mkt_true[idx], bs = beta_smb_true[idx],
    bh = beta_hml_true[idx], br = beta_rmw_true[idx], bc = beta_cma_true[idx],
    eps = rnorm(n(), 0, 0.04),
    ret_excess = bm*mkt_rf + bs*smb + bh*hml + br*rmw + bc*cma + eps,
    ret = ret_excess + rf,
    mktcap = exp(rnorm(n(), 22, 1)),
    siccd = c(2000, 3000, 6000)[group_id[idx]],
    ff12_code = c("Manuf","Hlth","Money")[group_id[idx]],
    ff12_num  = c(3, 10, 11)[group_id[idx]],
    ff12_name = ff12_code,
    gsector = c("20","35","40")[group_id[idx]],
    gics_sector_name = c("Industrials","Health Care","Financials")[group_id[idx]],
    ten_y = 0.025, ffr = 0.02, vix = 18,
    ticker = paste0("S", idx)
  ) |>
  select(permno, date_eom, ret, ret_excess, mktcap, siccd,
         ff12_num, ff12_code, ff12_name, gsector, gics_sector_name,
         mkt_rf, smb, hml, rmw, cma, rf, ten_y, ffr, vix, ticker)

CFG <- list(
  dev_mode = TRUE, dev_n_stocks = n_stocks,
  start_date = as.Date("2014-01-01"), end_date = as.Date("2024-12-31"),
  train_start = as.Date("2018-01-01"), train_end = as.Date("2022-12-31"),
  test_start  = as.Date("2023-01-01"), test_end  = as.Date("2024-12-31"),
  loading_window_months = 36, loading_min_obs = 24,
  loading_lag_months = 2, ff_lag_months = 2, macro_lag_months = 1,
  seed = 8675309
)
assign("CFG", CFG, envir = .GlobalEnv)

# ---- Source the actual modules ---------------------------------------------
source("R/05_features.R")
source("R/06_clustering.R")

cat("\n=== TEST 1: build_features() runs and produces alpha + lagged FFs ===\n")
feats <- build_features(panel)
stopifnot("alpha_realized" %in% names(feats))
stopifnot("alpha_fwd_1m" %in% names(feats))
stopifnot("mkt_rf_lag" %in% names(feats))
stopifnot("z_beta_mkt" %in% names(feats))
cat("rows: ", nrow(feats), "\n")
cat("pct beta_mkt non-NA: ", mean(!is.na(feats$beta_mkt)), "\n")
cat("pct alpha_realized non-NA: ", mean(!is.na(feats$alpha_realized)), "\n")
cat("pct alpha_fwd_1m non-NA: ", mean(!is.na(feats$alpha_fwd_1m)), "\n")

cat("\n=== TEST 2: leakage tests pass ===\n")
run_leakage_tests(feats)

cat("\n=== TEST 3: lag check — mkt_rf_lag at month t equals mkt_rf at t-2 ===\n")
chk <- feats |>
  arrange(permno, date_eom) |>
  group_by(permno) |>
  mutate(manual = lag(mkt_rf, 2)) |>
  ungroup() |>
  filter(!is.na(mkt_rf_lag), !is.na(manual))
cat("rows checked: ", nrow(chk), "\n")
cat("max abs diff: ", max(abs(chk$mkt_rf_lag - chk$manual)), "\n")
stopifnot(max(abs(chk$mkt_rf_lag - chk$manual)) < 1e-12)

cat("\n=== TEST 4: clustering produces assignments and the K is sane ===\n")
clust <- build_clusters_annual(panel, refit_window_months = 60,
                               k_grid = 2:6,
                               start_eval = as.Date("2018-01-01"),
                               end_eval   = as.Date("2024-12-31"))
print(clust$diag)
stopifnot(nrow(clust$assignments) > 0)
stopifnot(all(!is.na(clust$diag$k)))

cat("\n=== TEST 5: cluster recovery — does k=3 dominate? ===\n")
cat("Modal k chosen: ", names(sort(table(clust$diag$k), decreasing = TRUE))[1], "\n")

cat("\nALL TESTS PASSED\n")
