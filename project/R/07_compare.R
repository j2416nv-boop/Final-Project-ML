# ============================================================================
# R/07_compare.R
# Runs the head-to-head comparison of three classification schemes:
#   - GICS sectors (gsector / gics_sector_name)
#   - SIC-based FF12 industries (ff12_code)
#   - K-Means clusters on return co-movement (cluster_id)
#
# Four dimensions of comparison, all on the SAME holdout:
#
#   (a) COMPOSITION: contingency tables and adjusted Rand index between
#       each pair of groupings.
#   (b) HOMOGENEITY: within-group return correlation and alpha dispersion.
#       A "good" classification produces internally similar members.
#   (c) PREDICTIVE ACCURACY: an Elastic Net per group predicts alpha_fwd_1m.
#       Compare out-of-sample R^2, MAE, directional accuracy across schemes.
#   (d) ANOMALIES: Isolation Forest within each group flags bottom 5% on
#       the holdout. Compare overlap across schemes and forward-alpha
#       behaviour of flagged stocks at 1, 3, and 6 months.
# ============================================================================

suppressPackageStartupMessages({
  library(glmnet)         # elastic net
  library(isotree)        # isolation forest
  library(mclust)         # adjustedRandIndex
})

# ---- attach the cluster column to features ---------------------------------

attach_clusters <- function(feats, clusters) {
  feats |>
    dplyr::left_join(clusters, by = c("permno", "date_eom"))
}

# ---- composition: pairwise adjusted Rand + contingency --------------------

composition_compare <- function(df) {
  # Use the most recent month of the eval window so we get one row per
  # currently-listed name.
  d <- max(df$date_eom)
  snap <- df |>
    dplyr::filter(date_eom == d,
                  !is.na(gsector), !is.na(ff12_code), !is.na(cluster_id))

  pairs <- list(
    list(name = "GICS vs FF12",     a = snap$gsector,   b = snap$ff12_code),
    list(name = "GICS vs Cluster",  a = snap$gsector,   b = snap$cluster_id),
    list(name = "FF12 vs Cluster",  a = snap$ff12_code, b = snap$cluster_id)
  )
  ari <- purrr::map_dfr(pairs, function(p) {
    tibble::tibble(comparison = p$name,
                   adj_rand   = mclust::adjustedRandIndex(p$a, p$b),
                   n_stocks   = length(p$a))
  })

  contingency <- list(
    gics_vs_ff12    = table(snap$gics_sector_name, snap$ff12_code),
    gics_vs_cluster = table(snap$gics_sector_name, snap$cluster_id),
    ff12_vs_cluster = table(snap$ff12_code, snap$cluster_id)
  )

  list(ari = ari, contingency = contingency, snapshot_date = d, snapshot = snap)
}

# ---- homogeneity within a grouping ----------------------------------------
# For each group at each date, compute the cross-sectional sd of alpha_realized
# (lower = more homogeneous in unexplained returns) and the mean pairwise
# return correlation among members over the prior 24 months.

homogeneity_one <- function(df, group_col, panel_returns,
                            corr_window = 24, sample_pairs = 100) {
  df <- df |> dplyr::filter(!is.na(.data[[group_col]]),
                            !is.na(alpha_realized))

  alpha_sd <- df |>
    dplyr::group_by(date_eom, !!rlang::sym(group_col)) |>
    dplyr::summarise(alpha_sd = stats::sd(alpha_realized, na.rm = TRUE),
                     n = dplyr::n(), .groups = "drop") |>
    dplyr::filter(n >= 5) |>
    dplyr::summarise(mean_alpha_sd = mean(alpha_sd, na.rm = TRUE),
                     median_alpha_sd = stats::median(alpha_sd, na.rm = TRUE),
                     .groups = "drop")

  # mean within-group pairwise corr — sampled to keep compute reasonable
  set_seed(CFG$seed)
  group_rows <- df |>
    dplyr::distinct(permno, !!rlang::sym(group_col)) |>
    dplyr::rename(grp = !!rlang::sym(group_col))
  groups <- split(group_rows$permno, group_rows$grp)

  ret_wide <- panel_returns |>
    dplyr::select(permno, date_eom, ret_excess) |>
    tidyr::pivot_wider(names_from = permno, values_from = ret_excess) |>
    dplyr::arrange(date_eom)
  ret_mat <- as.matrix(ret_wide[, -1])
  storage.mode(ret_mat) <- "double"
  colnames(ret_mat) <- colnames(ret_wide)[-1]

  corrs <- purrr::map_dbl(groups, function(perms) {
    perms <- intersect(as.character(perms), colnames(ret_mat))
    if (length(perms) < 2) return(NA_real_)
    sub <- ret_mat[, perms, drop = FALSE]
    keep <- colSums(!is.na(sub)) >= corr_window
    sub <- sub[, keep, drop = FALSE]
    if (ncol(sub) < 2) return(NA_real_)
    cm <- stats::cor(sub, use = "pairwise.complete.obs")
    mean(cm[upper.tri(cm)], na.rm = TRUE)
  })

  list(
    alpha_sd_summary = alpha_sd,
    mean_within_group_corr = mean(corrs, na.rm = TRUE),
    median_within_group_corr = stats::median(corrs, na.rm = TRUE),
    n_groups_with_corr = sum(!is.na(corrs))
  )
}

homogeneity_compare <- function(df, panel_returns) {
  schemes <- c(GICS = "gics_sector_name",
               FF12 = "ff12_code",
               Cluster = "cluster_id")
  purrr::map_dfr(names(schemes), function(nm) {
    h <- homogeneity_one(df, schemes[[nm]], panel_returns)
    tibble::tibble(
      scheme = nm,
      mean_alpha_sd = h$alpha_sd_summary$mean_alpha_sd,
      median_alpha_sd = h$alpha_sd_summary$median_alpha_sd,
      mean_within_corr = h$mean_within_group_corr,
      median_within_corr = h$median_within_group_corr
    )
  })
}

# ---- prediction: per-group Elastic Net predicting alpha_fwd_1m ------------

FEATURE_COLS <- c(
  "z_beta_mkt", "z_beta_smb", "z_beta_hml", "z_beta_rmw", "z_beta_cma",
  "mom_12_1", "vol_12m", "log_mktcap_lag",
  "ten_y_lag", "ffr_lag", "vix_lag",
  "mkt_rf_lag", "smb_lag", "hml_lag", "rmw_lag", "cma_lag"
)

prepare_modeling_frame <- function(df) {
  df |>
    dplyr::filter(!is.na(alpha_fwd_1m)) |>
    dplyr::select(permno, date_eom, gics_sector_name, ff12_code, cluster_id,
                  alpha_fwd_1m,
                  dplyr::all_of(FEATURE_COLS)) |>
    tidyr::drop_na(dplyr::all_of(FEATURE_COLS))
}

# Fit one Elastic Net per group on train, predict on test, return per-row
# predictions plus per-group metrics.

fit_predict_groupwise <- function(train, test, group_col, alpha = 0.5,
                                  min_train_n = 60) {
  train <- train |> dplyr::filter(!is.na(.data[[group_col]]))
  test  <- test  |> dplyr::filter(!is.na(.data[[group_col]]))

  groups <- intersect(unique(train[[group_col]]), unique(test[[group_col]]))
  preds <- list()
  metrics <- list()
  for (g in groups) {
    tr <- train |> dplyr::filter(.data[[group_col]] == g)
    te <- test  |> dplyr::filter(.data[[group_col]] == g)
    if (nrow(tr) < min_train_n || nrow(te) == 0) next
    Xtr <- as.matrix(tr[, FEATURE_COLS]); ytr <- tr$alpha_fwd_1m
    Xte <- as.matrix(te[, FEATURE_COLS]); yte <- te$alpha_fwd_1m
    cv  <- tryCatch(
      glmnet::cv.glmnet(Xtr, ytr, alpha = alpha, nfolds = 5),
      error = function(e) NULL
    )
    if (is.null(cv)) next
    yhat <- as.numeric(predict(cv, Xte, s = "lambda.min"))
    preds[[as.character(g)]] <- te |>
      dplyr::select(permno, date_eom) |>
      dplyr::mutate(group = as.character(g), y_true = yte, y_hat = yhat)
    ss_res <- sum((yte - yhat)^2)
    ss_tot <- sum((yte - mean(yte))^2)
    metrics[[as.character(g)]] <- tibble::tibble(
      group = as.character(g),
      n_train = nrow(tr), n_test = nrow(te),
      r2_oos = if (ss_tot > 0) 1 - ss_res / ss_tot else NA_real_,
      mae    = mean(abs(yte - yhat)),
      dir_acc = mean(sign(yhat) == sign(yte))
    )
  }
  list(preds = dplyr::bind_rows(preds),
       metrics = dplyr::bind_rows(metrics))
}

# Pooled (no grouping) baseline.
fit_predict_pooled <- function(train, test, alpha = 0.5) {
  Xtr <- as.matrix(train[, FEATURE_COLS]); ytr <- train$alpha_fwd_1m
  Xte <- as.matrix(test[, FEATURE_COLS]);  yte <- test$alpha_fwd_1m
  cv  <- glmnet::cv.glmnet(Xtr, ytr, alpha = alpha, nfolds = 5)
  yhat <- as.numeric(predict(cv, Xte, s = "lambda.min"))
  ss_res <- sum((yte - yhat)^2); ss_tot <- sum((yte - mean(yte))^2)
  preds <- test |>
    dplyr::select(permno, date_eom) |>
    dplyr::mutate(group = "pooled", y_true = yte, y_hat = yhat)
  metrics <- tibble::tibble(
    scheme = "Pooled", group = "pooled",
    n_train = nrow(train), n_test = nrow(test),
    r2_oos = if (ss_tot > 0) 1 - ss_res / ss_tot else NA_real_,
    mae = mean(abs(yte - yhat)), dir_acc = mean(sign(yhat) == sign(yte))
  )
  list(preds = preds, metrics = metrics)
}

prediction_compare <- function(df) {
  mf <- prepare_modeling_frame(df)
  train <- mf |> dplyr::filter(date_eom >= CFG$train_start,
                               date_eom <= CFG$train_end)
  test  <- mf |> dplyr::filter(date_eom >= CFG$test_start,
                               date_eom <= CFG$test_end)
  log_msg("Prediction: train n=", nrow(train), ", test n=", nrow(test))

  schemes <- list(GICS    = "gics_sector_name",
                  FF12    = "ff12_code",
                  Cluster = "cluster_id")
  per_group <- purrr::map_dfr(names(schemes), function(nm) {
    res <- fit_predict_groupwise(train, test, schemes[[nm]])
    if (nrow(res$metrics) == 0) return(tibble::tibble())
    res$metrics |> dplyr::mutate(scheme = nm, .before = 1)
  })

  # Aggregate metrics by scheme, weighting by group size in test.
  agg <- per_group |>
    dplyr::group_by(scheme) |>
    dplyr::summarise(
      n_test_total = sum(n_test),
      r2_oos_weighted   = stats::weighted.mean(r2_oos,  w = n_test, na.rm = TRUE),
      mae_weighted      = stats::weighted.mean(mae,     w = n_test, na.rm = TRUE),
      dir_acc_weighted  = stats::weighted.mean(dir_acc, w = n_test, na.rm = TRUE),
      n_groups = dplyr::n(),
      .groups = "drop"
    )

  pooled <- fit_predict_pooled(train, test)
  pooled_agg <- pooled$metrics |>
    dplyr::transmute(scheme,
                     n_test_total = n_test,
                     r2_oos_weighted = r2_oos,
                     mae_weighted = mae,
                     dir_acc_weighted = dir_acc,
                     n_groups = 1L)

  list(
    per_group = per_group,
    aggregate = dplyr::bind_rows(agg, pooled_agg)
  )
}

# ---- anomaly detection: Isolation Forest within each group ----------------
# Trains an iForest within each group on the train window using FEATURE_COLS,
# scores every test-window row, flags bottom 5% (most anomalous). Then
# computes mean realized alpha at horizons +1, +3, +6 for flagged vs
# non-flagged within the same group.

attach_forward_alphas <- function(df, horizons = c(1, 3, 6)) {
  out <- df |> dplyr::arrange(permno, date_eom)
  for (h in horizons) {
    out <- out |>
      dplyr::group_by(permno) |>
      dplyr::mutate(!!paste0("alpha_fwd_", h, "m") :=
                      dplyr::lead(alpha_realized, h)) |>
      dplyr::ungroup()
  }
  out
}

anomaly_one_scheme <- function(df_with_fwd, group_col, train_idx, test_idx,
                               flag_q = 0.05, horizons = c(1, 3, 6),
                               min_train_n = 30) {
  df_with_fwd <- df_with_fwd |>
    dplyr::filter(!is.na(.data[[group_col]])) |>
    tidyr::drop_na(dplyr::all_of(FEATURE_COLS))
  train <- df_with_fwd[train_idx(df_with_fwd), ]
  test  <- df_with_fwd[test_idx(df_with_fwd),  ]

  groups <- intersect(unique(train[[group_col]]), unique(test[[group_col]]))
  flags <- list()
  for (g in groups) {
    tr <- train |> dplyr::filter(.data[[group_col]] == g)
    te <- test  |> dplyr::filter(.data[[group_col]] == g)
    if (nrow(tr) < min_train_n || nrow(te) == 0) next
    fit <- isotree::isolation.forest(
      as.matrix(tr[, FEATURE_COLS]),
      ntrees = 200, sample_size = min(256, nrow(tr)),
      ndim = 1, seed = CFG$seed
    )
    scores <- predict(fit, as.matrix(te[, FEATURE_COLS]))
    cutoff <- stats::quantile(scores, 1 - flag_q, na.rm = TRUE)
    flagged <- scores >= cutoff
    flags[[as.character(g)]] <- te |>
      dplyr::mutate(group = as.character(g),
                    iso_score = scores,
                    flagged = flagged)
  }
  flags_df <- dplyr::bind_rows(flags)

  # Forward alpha behaviour by flag status, per scheme.
  fwd_summary <- purrr::map_dfr(horizons, function(h) {
    col <- paste0("alpha_fwd_", h, "m")
    flags_df |>
      dplyr::filter(!is.na(.data[[col]])) |>
      dplyr::group_by(flagged) |>
      dplyr::summarise(
        horizon = h,
        n = dplyr::n(),
        mean_fwd_alpha = mean(.data[[col]], na.rm = TRUE),
        median_fwd_alpha = stats::median(.data[[col]], na.rm = TRUE),
        .groups = "drop"
      )
  })

  list(flags = flags_df, fwd_summary = fwd_summary)
}

anomaly_compare <- function(df) {
  df_fwd <- attach_forward_alphas(df)

  train_idx <- function(d) which(d$date_eom >= CFG$train_start &
                                 d$date_eom <= CFG$train_end)
  test_idx  <- function(d) which(d$date_eom >= CFG$test_start &
                                 d$date_eom <= CFG$test_end)

  schemes <- list(GICS    = "gics_sector_name",
                  FF12    = "ff12_code",
                  Cluster = "cluster_id")
  results <- purrr::imap(schemes, function(col, nm) {
    log_msg("  iForest within ", nm)
    anomaly_one_scheme(df_fwd, col, train_idx, test_idx)
  })

  # Cross-scheme overlap of flagged (permno, date_eom).
  flag_keys <- purrr::imap(results, function(r, nm) {
    if (nrow(r$flags) == 0) return(tibble::tibble())
    r$flags |>
      dplyr::filter(flagged) |>
      dplyr::transmute(permno, date_eom, scheme = nm)
  }) |> dplyr::bind_rows()

  pivot <- flag_keys |>
    dplyr::mutate(present = TRUE) |>
    tidyr::pivot_wider(names_from = scheme, values_from = present,
                       values_fill = FALSE)

  overlap <- if (nrow(pivot) == 0) {
    tibble::tibble()
  } else {
    cols <- intersect(c("GICS","FF12","Cluster"), names(pivot))
    purrr::map_dfr(cols, function(a) {
      purrr::map_dfr(cols, function(b) {
        if (a == b) return(tibble::tibble(a = a, b = b,
                                          n_a = sum(pivot[[a]]),
                                          n_b = sum(pivot[[b]]),
                                          jaccard = 1))
        inter <- sum(pivot[[a]] & pivot[[b]])
        un    <- sum(pivot[[a]] | pivot[[b]])
        tibble::tibble(a = a, b = b, n_a = sum(pivot[[a]]),
                       n_b = sum(pivot[[b]]),
                       jaccard = if (un > 0) inter / un else NA_real_)
      })
    })
  }

  fwd_compare <- purrr::imap_dfr(results, function(r, nm) {
    if (nrow(r$fwd_summary) == 0) return(tibble::tibble())
    r$fwd_summary |> dplyr::mutate(scheme = nm, .before = 1)
  })

  list(per_scheme = results,
       flag_overlap = overlap,
       fwd_compare = fwd_compare)
}
