# ============================================================================
# R/01_pull_crsp.R
#
# NOTE: this file's name is historical. The data source is Compustat
# (`comp.*`) because the WRDS seat used for this project does not include
# CRSP. The function names and the shape of the data frames returned by
# `pull_crsp_all()` are preserved so that downstream modules
# (04_build_panel.R etc.) continue to work unchanged. Conceptually:
#
#   permno     <- as.integer(gvkey)         # 6-digit gvkey reads fine as int
#   ret        <- trt1m / 100               # Compustat returns are in %
#   prc        <- prccm                     # monthly close
#   shrout     <- cshoq * 1000              # millions -> thousands (CRSP unit)
#   mktcap     <- |prc| * shrout            # in $thousands
#   delisting  <- comp.company.dldte        # no separate delisting return;
#                                           # trt1m already includes it
#   members    <- comp.idxcst_his where gvkeyx = '000003' (S&P 500 Comp-Ltd)
#   sic/ticker <- comp.names (historical, year1/year2 ranges)
#   gics       <- comp.company (current value; no PIT history available)
#
# The S&P 500 index identifier in Compustat is gvkeyx = '000003'
# (idx_index.conm = 'S&P 500 Comp-Ltd'). We hardcode it via SP500_GVKEYX.
# ============================================================================

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

SP500_GVKEYX <- "000003"

# ---- connection ------------------------------------------------------------

wrds_pgpass_candidates <- function() {
  candidates <- c(
    path.expand("~/.pgpass"),
    file.path(Sys.getenv("APPDATA"), "postgresql", "pgpass.conf")
  )
  unique(candidates[nzchar(candidates)])
}

wrds_pgpass_entry <- function() {
  for (path in wrds_pgpass_candidates()) {
    if (!file.exists(path)) next
    lines <- tryCatch(readLines(path, warn = FALSE), error = function(...) character())
    lines <- trimws(lines)
    lines <- lines[nzchar(lines) & !startsWith(lines, "#")]
    for (line in lines) {
      parts <- strsplit(line, ":", fixed = TRUE)[[1]]
      if (length(parts) < 5) next
      if (!identical(parts[[1]], "wrds-pgdata.wharton.upenn.edu")) next
      if (!identical(parts[[2]], "9737")) next
      if (!identical(parts[[3]], "wrds")) next
      return(list(
        user = parts[[4]],
        password = paste(parts[5:length(parts)], collapse = ":")
      ))
    }
  }
  NULL
}

wrds_connect <- function() {
  user <- if (exists("wrds_user", envir = .GlobalEnv)) {
    get("wrds_user", envir = .GlobalEnv)
  } else {
    Sys.getenv("WRDS_USER")
  }
  pwd <- if (exists("wrds_password", envir = .GlobalEnv)) {
    get("wrds_password", envir = .GlobalEnv)
  } else {
    Sys.getenv("WRDS_PASSWORD")
  }
  if (nchar(user) == 0) {
    stop("Set wrds_user and wrds_password before rendering. ",
         "Easiest: create credentials.R with those two assignments and ",
         "source() it from the setup chunk.")
  }
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = "wrds-pgdata.wharton.upenn.edu",
    port     = 9737,
    dbname   = "wrds",
    sslmode  = "require",
    user     = user,
    password = if (nchar(pwd) > 0) pwd else NULL
  )
}

# ---- 1. S&P 500 membership spans -------------------------------------------
# Compustat: comp.idxcst_his keyed on (gvkeyx, gvkey, iid). We restrict to
# the S&P 500 (gvkeyx='000003') and clip spans to [start_date, end_date].
# `thru` is NULL while still a member; we substitute end_date in that case.

pull_sp500_members <- function(start_date, end_date) {
  cache_parquet("sp500_members", function() {
    con <- wrds_connect(); on.exit(DBI::dbDisconnect(con))
    # Some firms have multiple share classes in idxcst_his (e.g. GOOG/GOOGL,
    # BRK.A/BRK.B). We keep the primary listing only — the lowest iid per
    # gvkey — so that permno = as.integer(gvkey) is unique downstream.
    sql <- '
      SELECT h.gvkey, h.iid, h."from" AS start_dt, h.thru AS end_dt
      FROM comp.idxcst_his h
      JOIN (
        SELECT gvkey, MIN(iid) AS iid
        FROM comp.idxcst_his
        WHERE gvkeyx = $3
        GROUP BY gvkey
      ) p ON h.gvkey = p.gvkey AND h.iid = p.iid
      WHERE h.gvkeyx = $3
        AND h."from" <= $2
        AND (h.thru IS NULL OR h.thru >= $1)
    '
    df <- DBI::dbGetQuery(
      con, sql,
      params = list(start_date, end_date, SP500_GVKEYX)
    )
    df |>
      dplyr::transmute(
        permno   = as.integer(gvkey),
        gvkey    = gvkey,
        iid      = iid,
        start    = as.Date(start_dt),
        end_date = dplyr::if_else(
          is.na(end_dt), as.Date(end_date), as.Date(end_dt)
        )
      )
  })
}

# ---- 2. Monthly returns + market cap ---------------------------------------
# Compustat monthly security file. trt1m is the total monthly return in
# percent (already includes dividends and adjusts for delisting in the
# delisting month), so we divide by 100 and don't need a separate delisting
# return frame. cshoq is in millions; we convert to thousands to match the
# CRSP convention so downstream code that consumes shrout/mktcap doesn't care
# about the data source.
#
# We pre-filter to S&P 500 issues via a join to idxcst_his to avoid pulling
# the entire secm table.

pull_msf <- function(start_date, end_date) {
  cache_parquet("crsp_msf", function() {
    con <- wrds_connect(); on.exit(DBI::dbDisconnect(con))
    sql <- "
      SELECT s.gvkey, s.iid, s.datadate,
             s.prccm, s.trt1m, s.cshoq, s.exchg
      FROM comp.secm s
      JOIN (
        SELECT gvkey, MIN(iid) AS iid
        FROM comp.idxcst_his
        WHERE gvkeyx = $3
        GROUP BY gvkey
      ) m
        ON s.gvkey = m.gvkey AND s.iid = m.iid
      WHERE s.datadate BETWEEN $1 AND $2
    "
    df <- DBI::dbGetQuery(
      con, sql,
      params = list(start_date, end_date, SP500_GVKEYX)
    )
    df |>
      dplyr::mutate(
        permno   = as.integer(gvkey),
        date     = as.Date(datadate),
        date_eom = eom(date),
        ret      = as.numeric(trt1m) / 100,
        prc      = as.numeric(prccm),
        # cshoq is in millions; CRSP shrout is in thousands. Multiply by 1000.
        shrout   = as.numeric(cshoq) * 1000,
        mktcap   = abs(prc) * shrout
      ) |>
      dplyr::select(permno, date_eom, ret, prc, shrout, mktcap)
  })
}

# ---- 3. Delisting returns --------------------------------------------------
# Compustat has no clean delisting-return analog. trt1m already incorporates
# the delisting month's total return, so the right thing to do is return an
# empty frame with the expected schema. Downstream pull_crsp_all() then
# falls through to ret_total = ret in the case_when().

pull_dlret <- function(start_date, end_date) {
  tibble::tibble(
    permno   = integer(0),
    date_eom = as.Date(character(0)),
    dlret    = numeric(0),
    dlstcd   = integer(0)
  )
}

# ---- 4. Stock header (ticker, SIC) -----------------------------------------
# comp.names is the historical name file: one row per (gvkey, year1..year2)
# span with company name, ticker, SIC, NAICS, and current GICS. We expose
# both a "latest record per gvkey" frame (for labelling) and a date-stamped
# SIC-history frame for point-in-time SIC assignment downstream.

pull_stocknames <- function() {
  cache_parquet("crsp_stocknames", function() {
    con <- wrds_connect(); on.exit(DBI::dbDisconnect(con))
    sql <- "
      SELECT gvkey, conm, tic, sic, year1, year2
      FROM comp.names
      WHERE gvkey IN (
        SELECT DISTINCT gvkey
        FROM comp.idxcst_his
        WHERE gvkeyx = $1
      )
    "
    df <- DBI::dbGetQuery(con, sql, params = list(SP500_GVKEYX))
    df |>
      dplyr::transmute(
        permno    = as.integer(gvkey),
        ticker    = tic,
        comnam    = conm,
        siccd     = suppressWarnings(as.integer(sic)),
        namedt    = as.Date(sprintf("%04d-01-01", as.integer(year1))),
        nameenddt = as.Date(sprintf("%04d-12-31", as.integer(year2))),
        # Compustat has no shrcd/exchcd analog at the names-history level.
        # Downstream code that filters on these treats NA as "unknown" and
        # passes through. The S&P 500 universe is common stock by index rule.
        shrcd     = NA_integer_,
        exchcd    = NA_integer_
      )
  })
}

pull_sic_history <- function() {
  cache_parquet("crsp_sic_history", function() {
    con <- wrds_connect(); on.exit(DBI::dbDisconnect(con))
    sql <- "
      SELECT gvkey, sic, year1, year2
      FROM comp.names
      WHERE sic IS NOT NULL
        AND gvkey IN (
          SELECT DISTINCT gvkey
          FROM comp.idxcst_his
          WHERE gvkeyx = $1
        )
    "
    df <- DBI::dbGetQuery(con, sql, params = list(SP500_GVKEYX))
    df |>
      dplyr::transmute(
        permno    = as.integer(gvkey),
        namedt    = as.Date(sprintf("%04d-01-01", as.integer(year1))),
        nameenddt = as.Date(sprintf("%04d-12-31", as.integer(year2))),
        siccd     = suppressWarnings(as.integer(sic))
      )
  })
}

# ---- 5. GICS sector --------------------------------------------------------
# In the CRSP build this came from CCM linkage + comp.company. We're already
# in Compustat, so we read GICS straight off comp.company. Same caveat as
# the original: comp.company stores CURRENT GICS only — there is no
# publicly available point-in-time GICS history here. linkdt/linkenddt are
# synthesised so the downstream join still works: linkdt is the firm's
# earliest S&P 500 entry date (or 1900 if absent), linkenddt is dldte if
# the firm has been deleted, otherwise far future.

pull_gics <- function() {
  cache_parquet("ccm_gics", function() {
    con <- wrds_connect(); on.exit(DBI::dbDisconnect(con))
    sql <- "
      SELECT c.gvkey,
             c.gsector, c.ggroup, c.gind, c.gsubind,
             c.dldte,
             m.first_in
      FROM comp.company c
      JOIN (
        SELECT gvkey, MIN(\"from\") AS first_in
        FROM comp.idxcst_his
        WHERE gvkeyx = $1
        GROUP BY gvkey
      ) m USING (gvkey)
    "
    df <- DBI::dbGetQuery(con, sql, params = list(SP500_GVKEYX))
    df |>
      dplyr::transmute(
        permno     = as.integer(gvkey),
        gvkey      = gvkey,
        linkdt     = dplyr::if_else(
          is.na(first_in), as.Date("1900-01-01"), as.Date(first_in)
        ),
        linkenddt  = dplyr::if_else(
          is.na(dldte), as.Date("9999-12-31"), as.Date(dldte)
        ),
        gsector    = gsector,
        ggroup     = ggroup,
        gind       = gind,
        gsubind    = gsubind
      )
  })
}

# ---- top-level orchestrator ------------------------------------------------

pull_crsp_all <- function() {
  start_date <- CFG$start_date
  end_date   <- CFG$end_date

  log_msg("Pulling Compustat data from ", start_date, " to ", end_date)

  members     <- pull_sp500_members(start_date, end_date)
  msf         <- pull_msf(start_date, end_date)
  dlret       <- pull_dlret(start_date, end_date)
  stocknames  <- pull_stocknames()
  sic_history <- pull_sic_history()
  gics        <- pull_gics()

  # In dev mode, restrict to a small permno subset for fast iteration.
  if (isTRUE(CFG$dev_mode)) {
    set_seed(CFG$seed)
    pool <- intersect(unique(members$permno), unique(msf$permno))
    keep <- sample(pool, min(CFG$dev_n_stocks, length(pool)))
    log_msg("DEV MODE: subsetting to ", length(keep), " permnos")
    members     <- dplyr::filter(members,     permno %in% keep)
    msf         <- dplyr::filter(msf,         permno %in% keep)
    dlret       <- dplyr::filter(dlret,       permno %in% keep)
    stocknames  <- dplyr::filter(stocknames,  permno %in% keep)
    sic_history <- dplyr::filter(sic_history, permno %in% keep)
    gics        <- dplyr::filter(gics,        permno %in% keep)
  }

  # Merge delisting return into msf. With Compustat as the source, dlret is
  # empty, so the case_when() collapses to ret_total = ret. We keep the
  # structure so the function output is shape-identical to the CRSP build.
  monthly <- msf |>
    dplyr::left_join(dlret, by = c("permno", "date_eom")) |>
    dplyr::mutate(
      ret_total = dplyr::case_when(
        !is.na(ret) & !is.na(dlret) ~ (1 + ret) * (1 + dlret) - 1,
        !is.na(ret)                 ~ ret,
        !is.na(dlret)               ~ dlret,
        TRUE                        ~ NA_real_
      )
    )

  # Most-recent-per-permno labels.
  stocknames_latest <- stocknames |>
    dplyr::group_by(permno) |>
    dplyr::slice_max(nameenddt, n = 1, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::select(permno, ticker, comnam)

  list(
    members     = members,
    monthly     = monthly,
    stocknames  = stocknames_latest,
    sic_history = sic_history,
    gics        = gics
  )
}
