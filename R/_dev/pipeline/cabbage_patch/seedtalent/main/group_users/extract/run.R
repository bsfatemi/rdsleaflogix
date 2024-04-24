box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[distinct, mutate],
  hcaconfig[dbc, dbd],
  hcaseedtalent[extract_group_users],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive],
  purrr[map_dfr]
)

# VARS --------------------------------------------------------------------
short_name <- args$short_name
out_table <- paste0(short_name, "_seedtalent_users")

# READ --------------------------------------------------------------------
json_files <- rd_raw_archive("group_users", short_name, "seedtalent")

# EXTRACT -----------------------------------------------------------------
group_users <- map_dfr(json_files, extract_group_users) |>
  distinct() |> # De-duplicate same user on different groups.
  mutate(run_date_utc = now("UTC"))

# LAND --------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
try(group_users <- check_cols(group_users, dbListFields(pg, out_table)))
dbWriteTable(pg, out_table, group_users, append = TRUE)
dbd(pg)
