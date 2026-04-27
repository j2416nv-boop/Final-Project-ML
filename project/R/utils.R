# ============================================================================
# R/utils.R
# Project-wide utilities: paths, parquet caching, logging, math helpers.
# Sourced by every other R module.
# ============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(arrow)
  library(lubridate)
  library(purrr)
  library(tibble)
  library(stringr)
  library(readr)
})

# ---- paths -----------------------------------------------------------------

PATHS <- list(
  raw       = "data/raw",
  processed = "data/processed",
  figures   = "figures"
)

for (p in PATHS) if (!dir.exists(p)) dir.create(p, recursive = TRUE, showWarnings = FALSE)

# ---- logging ---------------------------------------------------------------

log_msg <- function(...) {
  ts <- format(Sys.time(), "%H:%M:%S")
  cat(sprintf("[%s] %s\n", ts, paste0(..., collapse = "")))
}

# ---- parquet I/O wrappers (allow easy stubbing in tests) -------------------

write_parquet_safe <- function(x, path) {
  arrow::write_parquet(x, path)
}
read_parquet_safe <- function(path) {
  arrow::read_parquet(path)
}

# ---- parquet cache wrapper -------------------------------------------------
# Run a possibly-expensive function once, cache the result to parquet, and
# read from cache on subsequent runs unless force = TRUE.

cache_parquet <- function(name, fn, force = FALSE, dir = PATHS$raw) {
  path <- file.path(dir, paste0(name, ".parquet"))
  if (!force && file.exists(path)) {
    log_msg("cache hit:  ", path)
    return(read_parquet_safe(path))
  }
  log_msg("computing:  ", name)
  out <- fn()
  write_parquet_safe(out, path)
  log_msg("wrote:      ", path, "  (", format(nrow(out), big.mark = ","), " rows)")
  out
}

# ---- date helpers ----------------------------------------------------------

eom <- function(d) lubridate::ceiling_date(as.Date(d), "month") - 1

# ---- safe rolling regression ----------------------------------------------
# Estimate FF5 betas on a rolling window of monthly excess returns.
# Returns NAs when the window has fewer than min_obs valid (y, x) rows.

roll_ff5_betas <- function(y, X, min_obs = 24) {
  ok <- complete.cases(y, X)
  if (sum(ok) < min_obs) {
    return(c(alpha = NA_real_,
             beta_mkt = NA_real_, beta_smb = NA_real_,
             beta_hml = NA_real_, beta_rmw = NA_real_,
             beta_cma = NA_real_,
             rsq = NA_real_, n_obs = sum(ok)))
  }
  y <- y[ok]; X <- X[ok, , drop = FALSE]
  fit <- tryCatch(
    stats::lm.fit(cbind(1, as.matrix(X)), y),
    error = function(e) NULL
  )
  if (is.null(fit)) {
    return(c(alpha = NA_real_,
             beta_mkt = NA_real_, beta_smb = NA_real_,
             beta_hml = NA_real_, beta_rmw = NA_real_,
             beta_cma = NA_real_,
             rsq = NA_real_, n_obs = sum(ok)))
  }
  coefs <- fit$coefficients
  yhat  <- fit$fitted.values
  ss_res <- sum((y - yhat)^2)
  ss_tot <- sum((y - mean(y))^2)
  rsq <- if (ss_tot > 0) 1 - ss_res / ss_tot else NA_real_
  c(alpha    = unname(coefs[1]),
    beta_mkt = unname(coefs[2]),
    beta_smb = unname(coefs[3]),
    beta_hml = unname(coefs[4]),
    beta_rmw = unname(coefs[5]),
    beta_cma = unname(coefs[6]),
    rsq      = rsq,
    n_obs    = sum(ok))
}

# ---- cross-sectional z-score ----------------------------------------------
# Robust to a constant column (returns 0). Computed within each date.

cs_zscore <- function(x) {
  mu <- mean(x, na.rm = TRUE)
  sd <- stats::sd(x, na.rm = TRUE)
  if (!is.finite(sd) || sd == 0) return(rep(0, length(x)))
  (x - mu) / sd
}

# ---- safe winsorize at percentile ------------------------------------------

winsorize <- function(x, p = 0.01) {
  if (all(is.na(x))) return(x)
  lo <- stats::quantile(x, p,     na.rm = TRUE)
  hi <- stats::quantile(x, 1 - p, na.rm = TRUE)
  pmin(pmax(x, lo), hi)
}

# ---- pretty number formatters for tables ----------------------------------

fmt_pct <- function(x, digits = 2) sprintf(paste0("%.", digits, "f%%"), 100 * x)
fmt_num <- function(x, digits = 3) formatC(x, digits = digits, format = "f")

# ---- run-once seed setter --------------------------------------------------

set_seed <- function(seed = 8675309) {
  set.seed(seed)
  invisible(seed)
}
