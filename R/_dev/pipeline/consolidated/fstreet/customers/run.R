# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

# Source ------------------------------------------------------------------
source("inst/pipeline/consolidated/fstreet/customers/init.R", local = TRUE)
source(
  "inst/pipeline/consolidated/fstreet/customers/src/build_fstreet_supplemental_customers.R",
  local = TRUE
)

# Run ---------------------------------------------------------------------
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
fstreet <- pipelinetools::db_read_table_unique(
  pg, "fstreet_main_trz2_customers", "customer_id", "run_date_utc"
)
h80d <- pipelinetools::db_read_table_unique(
  pg, "fstreet_highway80_trz2_customers", "customer_id", "run_date_utc"
)
fstreet_supp <- pipelinetools::db_read_table_unique(
  pg, "fstreet_main_supplemental_customers", "Phone Number", "run_date_utc"
)
h80d_supp <- pipelinetools::db_read_table_unique(
  pg, "fstreet_highway80_supplemental_customers", "Phone Number", "run_date_utc"
)
DBI::dbDisconnect(pg)

fstreet$phone <- pipelinetools::process_phone(fstreet$phone)
h80d$phone <- pipelinetools::process_phone(h80d$phone)
fstreet_supp <- filter(
  fstreet_supp, !`Phone Number` %in% fstreet$phone & !`Phone Number` %in% h80d$phone
)
h80d_supp <- filter(h80d_supp, !`Phone Number` %in% fstreet$phone & !`Phone Number` %in% h80d$phone)

fstreet <- build_fstreet_supplemental_customers(fstreet, fstreet_supp)
h80d <- build_fstreet_supplemental_customers(h80d, h80d_supp)

fstreet$customer_id <- paste0("main-", fstreet$customer_id)
h80d$customer_id <- paste0("highway80-", h80d$customer_id)
trz_customers <- dplyr::bind_rows(fstreet, h80d)

# somehow there is bad data in their customer groups column, this will fix
trz_customers <- trz_customers %>% mutate(customer_groups = case_when(
  customer_groups == "SENIOR" ~ '[["SENIOR"]]',
  customer_groups == "VETERAN" ~ '[["VETERAN"]]',
  customer_groups == "EMPLOYEE" ~ '[["EMPLOYEE"]]',
  TRUE ~ customer_groups
))


customers <- hcatreez::build_trz2_customers(trz_customers, "fstreet")
customers$run_date_utc <- lubridate::now("UTC")

# Write -------------------------------------------------------------------
pg <- do.call(DBI::dbConnect, hcaconfig::ld_odbc("prod2", "consolidated"))
DBI::dbBegin(pg)
DBI::dbExecute(pg, paste("TRUNCATE TABLE", "fstreet_trz2_customers"))
DBI::dbWriteTable(pg, "fstreet_trz2_customers", customers, append = TRUE)
DBI::dbCommit(pg)
DBI::dbDisconnect(pg)
