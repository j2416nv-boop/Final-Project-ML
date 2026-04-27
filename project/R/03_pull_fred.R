# ============================================================================
# R/03_pull_fred.R
# Pulls macro controls from FRED (no API key required for public CSVs):
#   - DGS10  : 10-year Treasury constant maturity yield (%)
#   - DFF    : effective federal funds rate (%)
#   - VIXCLS : CBOE VIX close
# All published daily; we aggregate to end-of-month (last available value).
# ============================================================================

FRED_URL <- function(series_id) {
  sprintf("https://fred.stlouisfed.org/graph/fredgraph.csv?id=%s", series_id)
}

# ---- generic FRED CSV reader ------------------------------------------------

pull_fred_series <- function(series_id) {
  cache_parquet(paste0("fred_", tolower(series_id)), function() {
    df <- utils::read.csv(FRED_URL(series_id), stringsAsFactors = FALSE,
                          na.strings = c("", ".", "NA"))
    names(df) <- c("date", "value")
    df |>
      dplyr::transmute(
        date  = as.Date(date),
        value = suppressWarnings(as.numeric(value))
      ) |>
      dplyr::filter(!is.na(date))
  })
}

# ---- aggregate daily -> end-of-month ---------------------------------------

daily_to_eom <- function(df, value_name) {
  df |>
    dplyr::filter(!is.na(value)) |>
    dplyr::mutate(date_eom = eom(date)) |>
    dplyr::group_by(date_eom) |>
    dplyr::slice_max(date, n = 1, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::transmute(date_eom, !!value_name := value)
}

# ---- top-level orchestrator ------------------------------------------------

pull_fred_all <- function() {
  log_msg("Pulling FRED data")
  ten_y <- pull_fred_series("DGS10")  |> daily_to_eom("ten_y")
  ffr   <- pull_fred_series("DFF")    |> daily_to_eom("ffr")
  vix   <- pull_fred_series("VIXCLS") |> daily_to_eom("vix")

  ten_y |>
    dplyr::full_join(ffr, by = "date_eom") |>
    dplyr::full_join(vix, by = "date_eom") |>
    dplyr::filter(date_eom >= CFG$start_date,
                  date_eom <= CFG$end_date) |>
    dplyr::arrange(date_eom)
}
