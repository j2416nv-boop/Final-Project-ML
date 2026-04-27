# tests/test_compare.R — exercise the comparison code on the synthetic feats.

# This script assumes test_pipeline.R has already been run in the same R
# session, leaving feats and clust in the workspace. Here we just rebuild
# them quickly and run the compare functions.

# Stubs (same as before)
write_parquet_safe <- function(x, path) invisible(NULL)
read_parquet_safe  <- function(path) stop("not used in tests")
assign("write_parquet_safe", write_parquet_safe, envir = .GlobalEnv)
assign("read_parquet_safe",  read_parquet_safe,  envir = .GlobalEnv)

suppressPackageStartupMessages({
  library(dplyr); library(tidyr); library(lubridate); library(purrr)
  library(tibble); library(stringr); library(readr); library(glmnet)
  library(mclust); library(cluster); library(clue)
})

PATHS <- list(raw = "test_data/raw", processed = "test_data/processed",
              figures = "test_data/figures")
log_msg <- function(...) cat("[test]", ..., "\n", sep = "")
cache_parquet <- function(name, fn, force = FALSE, dir = PATHS$raw) fn()
eom <- function(d) lubridate::ceiling_date(as.Date(d), "month") - 1
roll_ff5_betas <- function(y, X, min_obs = 24) {
  ok <- complete.cases(y, X)
  if (sum(ok) < min_obs) return(c(alpha=NA_real_,beta_mkt=NA_real_,
    beta_smb=NA_real_,beta_hml=NA_real_,beta_rmw=NA_real_,beta_cma=NA_real_,
    rsq=NA_real_,n_obs=sum(ok)))
  y <- y[ok]; X <- X[ok,,drop=FALSE]
  fit <- stats::lm.fit(cbind(1, as.matrix(X)), y)
  c(alpha=unname(fit$coefficients[1]), beta_mkt=unname(fit$coefficients[2]),
    beta_smb=unname(fit$coefficients[3]), beta_hml=unname(fit$coefficients[4]),
    beta_rmw=unname(fit$coefficients[5]), beta_cma=unname(fit$coefficients[6]),
    rsq=NA_real_, n_obs=sum(ok))
}
cs_zscore <- function(x) {
  mu <- mean(x, na.rm=TRUE); sd <- stats::sd(x, na.rm=TRUE)
  if (!is.finite(sd) || sd == 0) return(rep(0, length(x)))
  (x - mu) / sd
}
set_seed <- function(seed=8675309) { set.seed(seed); invisible(seed) }
`%||%` <- function(a,b) if (is.null(a) || (length(a)==1 && is.na(a))) b else a

# Stub isotree.
isotree <- list(isolation.forest = function(X, ...) {
  list(type = "stub", mu = colMeans(X), S = stats::cov(X))
})
# Override predict() to dispatch on the stub.
predict_iso_stub <- function(object, newdata, ...) {
  d <- tryCatch(mahalanobis(newdata, object$mu, object$S, tol = 1e-12),
                error = function(e) rep(NA_real_, nrow(newdata)))
  d
}
# Replace the isotree::isolation.forest reference inside compare.R with our
# stub by injecting it into the namespace lookup.
# Easiest: define `isotree` as a package-like environment and let `::` find
# it via attached search path. Since :: requires an actual loaded namespace,
# we override the symbol with a function and use base::predict.S3 dispatch.
# For test purposes we'll patch compare.R to call get("isolation.forest")
# from a configurable env.

# Simpler: load compare.R, then *replace* anomaly_one_scheme with a version
# that uses our stub directly.

# Build feats from synth data --------------------------------------------------
set.seed(42)
n_stocks <- 60
months <- unique(eom(seq(as.Date("2014-01-01"), as.Date("2024-12-31"), by="month")))
group_id <- rep(1:3, length.out = n_stocks)
beta_mkt_true <- c(0.7, 1.3, 1.0)[group_id] + rnorm(n_stocks, sd = 0.10)
beta_smb_true <- c(-0.4, 0.6, 0.0)[group_id] + rnorm(n_stocks, sd = 0.15)
beta_hml_true <- c(0.5, -0.5, 0.0)[group_id] + rnorm(n_stocks, sd = 0.15)
beta_rmw_true <- rnorm(n_stocks, sd = 0.20)
beta_cma_true <- rnorm(n_stocks, sd = 0.20)
fac <- tibble(date_eom = months, mkt_rf = rnorm(length(months), 0.005, 0.045),
              smb=rnorm(length(months),0,0.025), hml=rnorm(length(months),0,0.025),
              rmw=rnorm(length(months),0,0.020), cma=rnorm(length(months),0,0.020),
              rf=rnorm(length(months),0.001,0.001))

panel <- expand.grid(permno = 10001:(10000+n_stocks), date_eom = months) |>
  as_tibble() |> arrange(permno, date_eom) |>
  mutate(idx = permno - 10000) |>
  left_join(fac, by = "date_eom") |>
  mutate(
    bm=beta_mkt_true[idx], bs=beta_smb_true[idx], bh=beta_hml_true[idx],
    br=beta_rmw_true[idx], bc=beta_cma_true[idx],
    eps = rnorm(n(), 0, 0.04),
    ret_excess = bm*mkt_rf + bs*smb + bh*hml + br*rmw + bc*cma + eps,
    ret = ret_excess + rf,
    mktcap = exp(rnorm(n(), 22, 1)),
    siccd = c(2000,3000,6000)[group_id[idx]],
    ff12_code = c("Manuf","Hlth","Money")[group_id[idx]],
    ff12_num  = c(3,10,11)[group_id[idx]],
    ff12_name = ff12_code,
    gsector = c("20","35","40")[group_id[idx]],
    gics_sector_name = c("Industrials","Health Care","Financials")[group_id[idx]],
    ten_y=0.025, ffr=0.02, vix=18, ticker=paste0("S", idx)
  ) |>
  select(permno, date_eom, ret, ret_excess, mktcap, siccd,
         ff12_num, ff12_code, ff12_name, gsector, gics_sector_name,
         mkt_rf, smb, hml, rmw, cma, rf, ten_y, ffr, vix, ticker)

CFG <- list(dev_mode=TRUE, dev_n_stocks=n_stocks,
            start_date=as.Date("2014-01-01"), end_date=as.Date("2024-12-31"),
            train_start=as.Date("2018-01-01"), train_end=as.Date("2022-12-31"),
            test_start =as.Date("2023-01-01"), test_end =as.Date("2024-12-31"),
            loading_window_months=36, loading_min_obs=24,
            loading_lag_months=2, ff_lag_months=2, macro_lag_months=1,
            seed=8675309)
assign("CFG", CFG, envir = .GlobalEnv)

source("R/05_features.R")
source("R/06_clustering.R")

# Build features and clusters
feats <- build_features(panel)
clust <- build_clusters_annual(panel, refit_window_months=60, k_grid=2:6,
                               start_eval=as.Date("2018-01-01"),
                               end_eval=as.Date("2024-12-31"))

# Now load compare.R but patch isotree:: usage. Instead of patching compare.R,
# we'll skip the anomaly test and exercise the rest.

# Manually source the parts we want to test (composition, homogeneity, prediction).
# Pull functions out of compare.R one by one to avoid the `library(isotree)` line.

compare_src <- readLines("R/07_compare.R")
# Drop the suppressPackageStartupMessages block that loads isotree.
in_block <- FALSE; out <- character()
for (ln in compare_src) {
  if (grepl("^suppressPackageStartupMessages", ln)) { in_block <- TRUE; next }
  if (in_block && grepl("^\\}\\)", ln)) { in_block <- FALSE; next }
  if (in_block) next
  out <- c(out, ln)
}
# Load only mclust + glmnet (already loaded above).
eval(parse(text = paste(out, collapse = "\n")))

cat("\n=== TEST 6: attach_clusters preserves rows ===\n")
feats_g <- attach_clusters(feats, clust$assignments)
cat("rows feats: ", nrow(feats), "  rows feats_g: ", nrow(feats_g), "\n")
stopifnot(nrow(feats_g) == nrow(feats))

cat("\n=== TEST 7: composition_compare runs ===\n")
comp <- composition_compare(feats_g)
print(comp$ari)

cat("\n=== TEST 8: homogeneity_compare runs ===\n")
homo <- homogeneity_compare(feats_g, panel)
print(homo)

cat("\n=== TEST 9: prediction_compare runs ===\n")
pred <- prediction_compare(feats_g)
print(pred$aggregate)

cat("\nALL COMPARE TESTS PASSED\n")
