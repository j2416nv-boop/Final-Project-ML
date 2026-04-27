# ============================================================================
# R/02_pull_french.R
# Downloads from Ken French's data library:
#   - FF5 factors monthly (Mkt-RF, SMB, HML, RMW, CMA) and risk-free rate
#   - FF12 industry definitions (SIC ranges -> industry code)
#
# Both are zipped CSVs with idiosyncratic formatting; we parse them carefully.
# ============================================================================

FRENCH_BASE <- "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
FF5_URL  <- file.path(FRENCH_BASE, "F-F_Research_Data_5_Factors_2x3_CSV.zip")
FF12_URL <- file.path(FRENCH_BASE, "Siccodes12.zip")

# ---- helpers ---------------------------------------------------------------

download_zip <- function(url, dest_dir) {
  if (!dir.exists(dest_dir)) dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
  zip_path <- file.path(dest_dir, basename(url))
  if (!file.exists(zip_path)) {
    log_msg("downloading ", url)
    utils::download.file(url, zip_path, mode = "wb", quiet = TRUE)
  }
  files <- utils::unzip(zip_path, exdir = dest_dir)
  files
}

# ---- FF5 factors (monthly) -------------------------------------------------
# The CSV has a header preamble, then a monthly block (YYYYMM dates),
# then an annual block. We keep monthly and stop at the first blank row.

pull_ff5 <- function() {
  cache_parquet("ff5", function() {
    files <- download_zip(FF5_URL, file.path(PATHS$raw, "french_ff5"))
    csv <- files[grepl("F-F_Research_Data_5_Factors_2x3\\.csv$", files, ignore.case = TRUE)]
    if (length(csv) == 0) csv <- files[grepl("\\.csv$", files, ignore.case = TRUE)][1]

    raw <- readLines(csv)

    # Find the header row (contains "Mkt-RF" and starts with whitespace).
    header_idx <- grep("Mkt-RF", raw)[1]
    # The monthly block ends at the first line after header that is empty
    # OR matches an annual line (4-digit year, no month).
    body_start <- header_idx + 1
    end_idx <- body_start
    while (end_idx <= length(raw)) {
      ln <- raw[end_idx]
      if (grepl("^\\s*$", ln)) break
      first_token <- trimws(strsplit(ln, ",")[[1]][1])
      # monthly tokens are 6 digits (YYYYMM); annual tokens are 4 digits.
      if (!grepl("^[0-9]{6}$", first_token)) break
      end_idx <- end_idx + 1
    }
    body <- raw[body_start:(end_idx - 1)]

    df <- utils::read.csv(text = c(raw[header_idx], body),
                          stringsAsFactors = FALSE, check.names = FALSE)

    names(df)[1] <- "yyyymm"
    df |>
      dplyr::transmute(
        date_eom = eom(as.Date(paste0(yyyymm, "01"), "%Y%m%d")),
        mkt_rf   = as.numeric(`Mkt-RF`) / 100,
        smb      = as.numeric(SMB) / 100,
        hml      = as.numeric(HML) / 100,
        rmw      = as.numeric(RMW) / 100,
        cma      = as.numeric(CMA) / 100,
        rf       = as.numeric(RF) / 100
      ) |>
      dplyr::filter(!is.na(date_eom)) |>
      dplyr::arrange(date_eom)
  })
}

# ---- FF12 industry definitions --------------------------------------------
# Siccodes12.txt is a fixed-format text file:
#   "  1 NoDur  Consumer NonDurables ..."  (header line per industry)
#   "          0100-0299"                  (one or more SIC range lines)
# We parse it into a long table: industry_num, industry_code, industry_name,
# sic_lo, sic_hi.

pull_ff12_def <- function() {
  cache_parquet("ff12_def", function() {
    files <- download_zip(FF12_URL, file.path(PATHS$raw, "french_ff12"))
    txt <- files[grepl("Siccodes12\\.txt$", files, ignore.case = TRUE)]
    if (length(txt) == 0) txt <- files[grepl("\\.txt$", files, ignore.case = TRUE)][1]

    raw <- readLines(txt)

    rows <- list()
    cur_num <- NA_integer_
    cur_code <- NA_character_
    cur_name <- NA_character_

    # Industry header pattern: "  1 NoDur  Consumer NonDurables..."
    hdr_re <- "^\\s*([0-9]+)\\s+([A-Za-z]+)\\s+(.*)$"
    # SIC range pattern: "          0100-0299" (optionally followed by a name)
    rng_re <- "^\\s*([0-9]{4})-([0-9]{4})"

    for (ln in raw) {
      if (grepl("^\\s*$", ln)) next
      if (grepl(rng_re, ln)) {
        m <- regmatches(ln, regexec(rng_re, ln))[[1]]
        rows[[length(rows) + 1]] <- tibble::tibble(
          industry_num  = cur_num,
          industry_code = cur_code,
          industry_name = cur_name,
          sic_lo        = as.integer(m[2]),
          sic_hi        = as.integer(m[3])
        )
      } else if (grepl(hdr_re, ln)) {
        m <- regmatches(ln, regexec(hdr_re, ln))[[1]]
        cur_num  <- as.integer(m[2])
        cur_code <- m[3]
        cur_name <- trimws(m[4])
      }
    }

    dplyr::bind_rows(rows)
  })
}

# ---- assignment helper -----------------------------------------------------
# Given a vector of SIC codes, return the FF12 industry_num / industry_code.
# Codes that fall in no range are assigned to industry 12 ("Other"), per the
# Ken French convention.

assign_ff12 <- function(sic, ff12_def) {
  out <- tibble::tibble(siccd = as.integer(sic),
                        ff12_num  = NA_integer_,
                        ff12_code = NA_character_,
                        ff12_name = NA_character_)
  # Build a lookup vector of length max_sic; fill in industries by range.
  max_sic <- max(c(ff12_def$sic_hi, out$siccd), na.rm = TRUE)
  num_lookup  <- rep(NA_integer_,   max_sic + 1)
  code_lookup <- rep(NA_character_, max_sic + 1)
  name_lookup <- rep(NA_character_, max_sic + 1)
  for (k in seq_len(nrow(ff12_def))) {
    rng <- (ff12_def$sic_lo[k] + 1):(ff12_def$sic_hi[k] + 1)  # +1 for 1-based
    num_lookup[rng]  <- ff12_def$industry_num[k]
    code_lookup[rng] <- ff12_def$industry_code[k]
    name_lookup[rng] <- ff12_def$industry_name[k]
  }
  ok <- !is.na(out$siccd) & out$siccd >= 0 & out$siccd <= max_sic
  out$ff12_num [ok] <- num_lookup [out$siccd[ok] + 1]
  out$ff12_code[ok] <- code_lookup[out$siccd[ok] + 1]
  out$ff12_name[ok] <- name_lookup[out$siccd[ok] + 1]
  # Assign "Other" (12) to anything still NA but with valid SIC.
  miss <- !is.na(out$siccd) & is.na(out$ff12_num)
  out$ff12_num [miss] <- 12L
  out$ff12_code[miss] <- "Other"
  out$ff12_name[miss] <- "Other"
  out
}

# ---- top-level orchestrator ------------------------------------------------

pull_french_all <- function() {
  log_msg("Pulling Ken French data")
  list(
    ff5      = pull_ff5(),
    ff12_def = pull_ff12_def()
  )
}
