# ============================================================================
# R/06_clustering.R
# Builds K-Means clusters of S&P 500 stocks based on RETURN CO-MOVEMENT,
# NOT on FF5 factor loadings. The professor's second comment makes the FF5-
# loadings approach problematic: "high factor exposure -> high return" is
# tautological, and it conflates a risk taxonomy with an industry taxonomy.
# A correlation-based clustering treats two stocks as similar if their
# returns move together over time, which is a clean industry-style signal
# that is independent of the FF5 risk model.
#
# Procedure:
#   1. For each fitting date, take the 60-month window of monthly excess
#      returns ending at the fitting date. Keep stocks with >= 36 months of
#      data; missing values get pairwise complete handling for the corr.
#   2. Compute the n x n correlation matrix; the embedding for each stock
#      is its column of the correlation matrix (so each stock is described
#      by its correlation with every other stock — high-dim but stable).
#      Reduce with PCA to k_pc = 10 components.
#   3. Run K-Means for k = 2..k_max; pick k by silhouette + elbow.
#   4. Refit annually. Use Hungarian-algorithm label matching against the
#      previous fit to keep cluster IDs stable across periods.
#
# Output: a long table (permno, date_eom, cluster_id) covering every month
# in the analysis range, where cluster_id is the cluster assignment used at
# decision time t (i.e. estimated from data ending at or before t).
# ============================================================================

suppressPackageStartupMessages({
  library(cluster)        # silhouette
  library(clue)           # solve_LSAP for label matching
})

# ---- build the per-stock embedding from a return panel slice --------------
# Input: long table (permno, date_eom, ret_excess) covering the window.
# Output: matrix (rows = permnos, cols = "feature i"), where feature i is
# the correlation of this stock's returns with stock i.

build_corr_embedding <- function(window_panel) {
  wide <- window_panel |>
    dplyr::select(permno, date_eom, ret_excess) |>
    tidyr::pivot_wider(names_from = permno, values_from = ret_excess) |>
    dplyr::arrange(date_eom)
  mat <- as.matrix(wide[, -1])
  storage.mode(mat) <- "double"

  # Drop columns (stocks) with too few non-missing observations.
  keep <- colSums(!is.na(mat)) >= 36
  mat <- mat[, keep, drop = FALSE]
  if (ncol(mat) < 5) {
    return(list(emb = matrix(numeric(0), 0, 0), permnos = integer(0)))
  }

  cor_mat <- stats::cor(mat, use = "pairwise.complete.obs")
  cor_mat[is.na(cor_mat)] <- 0  # rare pairs with no overlap

  list(emb = cor_mat, permnos = as.integer(colnames(cor_mat)))
}

# ---- pick k by silhouette over a small grid -------------------------------

pick_k <- function(emb, k_grid = 2:10, seed = 8675309) {
  set.seed(seed)
  # PCA reduce for speed and stability; 10 PCs typically capture most signal.
  k_pc <- min(10, ncol(emb) - 1, nrow(emb) - 1)
  if (k_pc < 2) return(list(k = NA_integer_, sil = NA_real_, scores = NULL))
  pc <- stats::prcomp(emb, center = TRUE, scale. = FALSE)
  X <- pc$x[, seq_len(k_pc), drop = FALSE]

  scores <- purrr::map_dfr(k_grid, function(k) {
    fit <- stats::kmeans(X, centers = k, nstart = 25, iter.max = 100)
    sil <- if (k >= 2 && k < nrow(X)) {
      mean(cluster::silhouette(fit$cluster, stats::dist(X))[, "sil_width"])
    } else NA_real_
    tibble::tibble(k = k, wss = fit$tot.withinss, sil = sil)
  })

  best <- scores |> dplyr::slice_max(sil, n = 1, with_ties = FALSE)
  list(k = best$k, sil = best$sil, scores = scores, pc = X)
}

# ---- single-period clustering ---------------------------------------------

cluster_one_period <- function(window_panel, k_grid = 2:10, k_force = NULL,
                               seed = 8675309) {
  emb_obj <- build_corr_embedding(window_panel)
  if (length(emb_obj$permnos) < 5) {
    return(list(assignments = tibble::tibble(permno = integer(),
                                             cluster_id = integer()),
                k = NA_integer_, sil = NA_real_, scores = NULL,
                centers = NULL, X = NULL, permnos = integer()))
  }

  pick <- pick_k(emb_obj$emb, k_grid = k_grid, seed = seed)
  k_use <- if (!is.null(k_force)) k_force else pick$k

  set.seed(seed)
  fit <- stats::kmeans(pick$pc, centers = k_use, nstart = 50, iter.max = 200)

  list(
    assignments = tibble::tibble(permno = emb_obj$permnos,
                                 cluster_id = as.integer(fit$cluster)),
    k = k_use, sil = pick$sil, scores = pick$scores,
    centers = fit$centers, X = pick$pc, permnos = emb_obj$permnos
  )
}

# ---- Hungarian label matching ---------------------------------------------
# Re-label new clusters so the Jaccard overlap with old clusters is
# maximised, keeping IDs stable across refits.

match_cluster_labels <- function(prev_assign, new_assign) {
  if (is.null(prev_assign) || nrow(prev_assign) == 0) return(new_assign)
  joined <- dplyr::inner_join(
    dplyr::rename(prev_assign, prev_cluster = cluster_id),
    dplyr::rename(new_assign,  new_cluster  = cluster_id),
    by = "permno"
  )
  if (nrow(joined) == 0) return(new_assign)

  prev_ids <- sort(unique(joined$prev_cluster))
  new_ids  <- sort(unique(joined$new_cluster))
  # cost matrix: rows = new_ids, cols = prev_ids; cost = -Jaccard
  jac <- function(a, b) {
    A <- joined$permno[joined$new_cluster  == a]
    B <- joined$permno[joined$prev_cluster == b]
    length(intersect(A, B)) / length(union(A, B))
  }
  M <- matrix(NA_real_, nrow = length(new_ids), ncol = length(prev_ids))
  for (i in seq_along(new_ids)) for (j in seq_along(prev_ids)) {
    M[i, j] <- jac(new_ids[i], prev_ids[j])
  }
  # Pad to square for solve_LSAP.
  pad_n <- max(nrow(M), ncol(M))
  M_pad <- matrix(0, pad_n, pad_n)
  M_pad[seq_len(nrow(M)), seq_len(ncol(M))] <- M
  sol <- clue::solve_LSAP(M_pad, maximum = TRUE)
  remap <- integer(0)
  for (i in seq_along(new_ids)) {
    j <- as.integer(sol[i])
    remap[as.character(new_ids[i])] <- if (j <= length(prev_ids)) {
      prev_ids[j]
    } else {
      max(c(prev_ids, names(remap)) |> as.integer(), 0L) + i
    }
  }
  new_assign |>
    dplyr::mutate(cluster_id = remap[as.character(cluster_id)] |> as.integer())
}

# ---- annual refit orchestrator --------------------------------------------
# Refits at the end of each calendar year using the prior 60 months. The
# resulting assignments are forward-applied: clusters fit on data ending
# Dec YYYY are used for all months of YYYY+1.

build_clusters_annual <- function(panel, refit_window_months = 60,
                                  k_grid = 2:10, k_force = NULL,
                                  start_eval = NULL, end_eval = NULL) {
  log_msg("Building annual K-Means clusters on return co-movement")
  if (is.null(start_eval)) start_eval <- CFG$train_start
  if (is.null(end_eval))   end_eval   <- CFG$end_date

  # Refit dates: end of each year preceding an evaluation year.
  refit_years <- seq(lubridate::year(start_eval) - 1, lubridate::year(end_eval) - 1)
  refit_dates <- eom(as.Date(paste0(refit_years, "-12-31")))

  panel_use <- panel |> dplyr::select(permno, date_eom, ret_excess)
  prev_assign <- NULL
  diagnostics <- list()
  all_assign  <- list()

  for (rd in seq_along(refit_dates)) {
    d <- refit_dates[rd]
    win_start <- eom(d %m-% months(refit_window_months - 1))
    slice <- dplyr::filter(panel_use, date_eom >= win_start, date_eom <= d)
    res <- cluster_one_period(slice, k_grid = k_grid, k_force = k_force,
                              seed = CFG$seed + rd)
    if (nrow(res$assignments) == 0) {
      log_msg("  refit ", d, ": insufficient data, skipping")
      next
    }
    matched <- match_cluster_labels(prev_assign, res$assignments)
    prev_assign <- matched

    # Apply this assignment to all months of the FOLLOWING year.
    # Anchor seq() to the first of each month to avoid day-31 + 1mo drift.
    apply_year <- lubridate::year(d) + 1
    apply_months <- unique(eom(seq(as.Date(paste0(apply_year, "-01-01")),
                                   as.Date(paste0(apply_year, "-12-01")),
                                   by = "month")))
    apply_months <- apply_months[apply_months <= eom(end_eval)]
    if (length(apply_months) == 0) next

    grid <- tidyr::expand_grid(date_eom = apply_months,
                               permno   = matched$permno) |>
      dplyr::left_join(matched, by = "permno")
    all_assign[[as.character(d)]] <- grid

    diagnostics[[as.character(d)]] <- tibble::tibble(
      refit_date  = d,
      k           = res$k,
      silhouette  = res$sil,
      n_stocks    = nrow(matched)
    )
    log_msg(sprintf("  refit %s: k=%d, sil=%.3f, n=%d",
                    format(d), res$k, res$sil %||% NA_real_, nrow(matched)))
  }

  cluster_panel <- dplyr::bind_rows(all_assign)
  diag_df <- dplyr::bind_rows(diagnostics)

  write_parquet_safe(cluster_panel,
                       file.path(PATHS$processed, "clusters_annual.parquet"))
  write_parquet_safe(diag_df,
                       file.path(PATHS$processed, "clusters_diag.parquet"))

  list(assignments = cluster_panel, diag = diag_df)
}

# null-coalesce
`%||%` <- function(a, b) if (is.null(a) || (length(a) == 1 && is.na(a))) b else a
