# Marketing Studio App Data
# run.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

# Source ------------------------------------------------------------------
org <- "demo"
source(
  stringr::str_glue("inst/pipeline/appdata/polaris/polaris_appdata/custom/{org}/init.R"),
  local = TRUE
)
source(
  stringr::str_glue(
    "inst/pipeline/appdata/polaris/polaris_appdata/custom/{org}/src/build_{org}_polaris.R"
  ),
  local = TRUE
)

# Read ---------------------------------------------------------------
org <- "medithrive"

# READ FROM DATABASE
pg <- do.call(DBI::dbConnect, hcaconfig::ld_odbc("prod2", "integrated"))
order_info <- dplyr::tbl(pg, "customer_behavior") %>%
  dplyr::filter(org == !!org) %>%
  dplyr::collect()
raw_customers <- dplyr::tbl(pg, "customers") %>%
  dplyr::filter(org == !!org) %>%
  dplyr::collect()
DBI::dbDisconnect(pg)

# Run ---------------------------------------------------------------
customers <- build_demo_polaris(order_info, raw_customers)

# Write ---------------------------------------------------------------
if (nrow(customers) < 50) {
  stop("TOO FEW CUSTOMERS")
} else {
  customers$run_date_utc <- now(tzone = "UTC")
  pg <- hcaconfig::dbc("prod2", "appdata")
  DBI::dbBegin(pg)
  DBI::dbExecute(pg, "DELETE FROM polaris_appdata WHERE org = 'demo'")
  DBI::dbAppendTable(pg, "polaris_appdata", value = customers)
  DBI::dbExecute(pg, "REINDEX TABLE polaris_appdata")
  DBI::dbExecute(pg, "ANALYZE polaris_appdata")
  DBI::dbCommit(pg)
  DBI::dbExecute(pg, "VACUUM polaris_appdata")
  hcaconfig::dbd(pg)

  # Push final customers' opt status.
  org_uuid <- hcaconfig::lookupOrgGuid("demo")
  optin_status <- transmute(
    filter(customers, !is.na(last_customer_phone)),
    org = org, orguuid = !!org_uuid, phone = last_customer_phone, ts = updated_at,
    status = if_else(last_opted_in == "Y", "opt_in", "opt_out")
  ) |>
    arrange(desc(ts)) |>
    distinct(phone, .keep_all = TRUE)
  pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "integrated")
  DBI::dbBegin(pg)
  DBI::dbExecute(
    pg, stringr::str_glue("DELETE FROM custom_code_optin_status WHERE orguuid = '{org_uuid}'")
  )
  DBI::dbWriteTable(pg, "custom_code_optin_status", optin_status, append = TRUE)
  DBI::dbCommit(pg)
  hcaconfig::dbd(pg)
}
