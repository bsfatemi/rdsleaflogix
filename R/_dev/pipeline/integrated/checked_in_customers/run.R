# Consolidated Checked-in Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

# Libraries.
box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable, SQL],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  purrr[map_dfr],
  stringr[str_to_lower, str_to_upper]
)

# Vars --------------------------------------------------------------------
pos_systems <- c("treez", "blaze", "leaflogix")

# Read and merge ----------------------------------------------------------
# cfg to connect in `hcaconfig::dbc`, "prod2" by default if "HCA_ENV" not set.
(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
cabbage_patch <- dbc(db_cfg, "cabbage_patch")
checked_in <- map_dfr(pos_systems, function(pos) {
  dbReadTable(cabbage_patch, paste0(pos, "_checked_in_customers"))
}) |>
  mutate(full_name = str_to_upper(full_name), email = str_to_lower(email))
dbd(cabbage_patch)

# Write -------------------------------------------------------------------
pg <- dbc(db_cfg, "appdata")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", SQL("clair.checked_in_customers")))
dbWriteTable(pg, SQL("clair.checked_in_customers"), checked_in, append = TRUE)
dbCommit(pg)
dbd(pg)
