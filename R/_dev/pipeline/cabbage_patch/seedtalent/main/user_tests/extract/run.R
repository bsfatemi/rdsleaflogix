box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcaseedtalent[extract_user_tests],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive],
  purrr[map_dfr]
)

# VARS --------------------------------------------------------------------
short_name <- args$short_name
out_table <- paste0(short_name, "_seedtalent_user_tests")

# READ --------------------------------------------------------------------
json_files <- rd_raw_archive("user_tests", short_name, "seedtalent")

# EXTRACT -----------------------------------------------------------------
user_tests <- map_dfr(json_files, extract_user_tests) |>
  mutate(run_date_utc = now("UTC"))

# LAND --------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
try(user_tests <- check_cols(user_tests, dbListFields(pg, out_table)))
dbWriteTable(pg, out_table, user_tests, append = TRUE)
dbd(pg)
