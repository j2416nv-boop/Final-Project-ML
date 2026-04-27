# ============================================================================
# R/04_build_panel.R
# Builds the master monthly panel: one row per (permno, date_eom) for every
# month the stock was an S&P 500 constituent. Attaches:
#   - returns (with delisting adjustment) and excess returns
#   - point-in-time SIC -> FF12 industry
#   - point-in-time GICS sector via CCM
#   - FF5 factor returns (raw, contemporaneous — features will lag them)
#   - macro controls (raw, contemporaneous — features will lag them)
# ============================================================================

# GICS sector code -> human name (the standard 11 sectors).
GICS_NAMES <- c(
  "10" = "Energy",
  "15" = "Materials",
  "20" = "Industrials",
  "25" = "Consumer Discretionary",
  "30" = "Consumer Staples",
  "35" = "Health Care",
  "40" = "Financials",
  "45" = "Information Technology",
  "50" = "Communication Services",
  "55" = "Utilities",
  "60" = "Real Estate"
)

# ---- expand membership spans into one row per (permno, date_eom) -----------

expand_membership <- function(members, all_eom_dates) {
  members |>
    dplyr::rowwise() |>
    dplyr::mutate(
      months = list(all_eom_dates[all_eom_dates >= eom(start) &
                                  all_eom_dates <= eom(end_date)])
    ) |>
    dplyr::ungroup() |>
    tidyr::unnest(months) |>
    dplyr::transmute(permno, date_eom = months) |>
    dplyr::distinct()
}

# ---- assign point-in-time SIC at each date_eom -----------------------------
# stocknames has overlapping (namedt, nameenddt) ranges per permno. For each
# (permno, date_eom) we pick the SIC whose range contains date_eom. If
# multiple match, take the latest namedt.

assign_pit_sic <- function(panel_keys, sic_history) {
  # Index sic_history by permno for speed.
  sic_by_p <- split(sic_history, sic_history$permno)
  panel_keys |>
    dplyr::group_by(permno) |>
    dplyr::group_modify(function(grp, key) {
      h <- sic_by_p[[as.character(key$permno)]]
      if (is.null(h) || nrow(h) == 0) {
        grp$siccd <- NA_integer_
        return(grp)
      }
      # For each date, find rows where namedt <= date <= nameenddt.
      grp$siccd <- vapply(grp$date_eom, function(d) {
        ok <- h$namedt <= d & h$nameenddt >= d
        if (!any(ok)) return(NA_integer_)
        # latest namedt among matches
        idx <- which(ok)
        idx <- idx[which.max(h$namedt[idx])]
        h$siccd[idx]
      }, integer(1))
      grp
    }) |>
    dplyr::ungroup()
}

# ---- assign point-in-time GICS at each date_eom ----------------------------

assign_pit_gics <- function(panel_keys, gics) {
  gics_by_p <- split(gics, gics$permno)
  panel_keys |>
    dplyr::group_by(permno) |>
    dplyr::group_modify(function(grp, key) {
      h <- gics_by_p[[as.character(key$permno)]]
      if (is.null(h) || nrow(h) == 0) {
        grp$gsector <- NA_character_
        return(grp)
      }
      grp$gsector <- vapply(grp$date_eom, function(d) {
        ok <- h$linkdt <= d & h$linkenddt >= d
        if (!any(ok)) return(NA_character_)
        idx <- which(ok)
        idx <- idx[which.max(h$linkdt[idx])]
        as.character(h$gsector[idx])
      }, character(1))
      grp
    }) |>
    dplyr::ungroup()
}

# ---- top-level builder -----------------------------------------------------

build_panel <- function(crsp, french, fred) {
  log_msg("Building master panel")

  # All end-of-month dates in the analysis range. seq(by = "month") from a
  # last-of-month start preserves the day-of-month, which after eom() can
  # collapse adjacent months to duplicates (e.g. 2014-01-31 + 1mo = 2014-02-28
  # which eom-rounds to 2014-02-28, then 2014-03-28 also rounds to 2014-03-31
  # — but if we start from a non-last-day date, eom() can map two seq()
  # outputs to the same end-of-month). Anchor to the first of the month and
  # then snap to eom + dedupe defensively.
  start_first <- lubridate::floor_date(CFG$start_date, "month")
  end_first   <- lubridate::floor_date(CFG$end_date,   "month")
  all_eom <- unique(eom(seq(start_first, end_first, by = "month")))

  # 1. Skeleton: (permno, date_eom) for every constituent-month.
  panel_keys <- expand_membership(crsp$members, all_eom)
  log_msg("  panel keys: ", nrow(panel_keys), " rows")

  # 2. Attach point-in-time SIC.
  panel_keys <- assign_pit_sic(panel_keys, crsp$sic_history)

  # 3. Map SIC -> FF12.
  ff12_assignment <- assign_ff12(panel_keys$siccd, french$ff12_def)
  panel_keys <- dplyr::bind_cols(panel_keys, ff12_assignment[, c("ff12_num", "ff12_code", "ff12_name")])

  # 4. Attach point-in-time GICS.
  panel_keys <- assign_pit_gics(panel_keys, crsp$gics)
  panel_keys$gics_sector_name <- GICS_NAMES[panel_keys$gsector]

  # 5. Attach returns and market cap.
  panel <- panel_keys |>
    dplyr::left_join(
      crsp$monthly |> dplyr::select(permno, date_eom, ret_total, mktcap),
      by = c("permno", "date_eom")
    ) |>
    dplyr::rename(ret = ret_total)

  # 6. Attach FF5 factors and risk-free rate; compute excess return.
  panel <- panel |>
    dplyr::left_join(french$ff5, by = "date_eom") |>
    dplyr::mutate(ret_excess = ret - rf)

  # 7. Attach macro controls.
  panel <- panel |>
    dplyr::left_join(fred, by = "date_eom")

  # 8. Attach ticker for readability.
  panel <- panel |>
    dplyr::left_join(crsp$stocknames, by = "permno") |>
    dplyr::arrange(permno, date_eom)

  write_parquet_safe(panel, file.path(PATHS$processed, "panel.parquet"))
  log_msg("  wrote: ", file.path(PATHS$processed, "panel.parquet"))
  panel
}
