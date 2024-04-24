box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcaseedtalent[extract_groups],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
integrator_id <- args$integrator_id
out_table <- "seedtalent_groups"

# READ --------------------------------------------------------------------
json_files <- rd_raw_archive("groups", paste("all", integrator_id, sep = "_"), "seedtalent")

# EXTRACT -----------------------------------------------------------------
groups <- extract_groups(json_files) |>
  mutate(
    integrator_id = integrator_id,
    run_date_utc = now("UTC")
  )

# LAND --------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
try(groups <- check_cols(groups, dbListFields(pg, out_table)))
dbWriteTable(pg, out_table, groups, append = TRUE)
dbd(pg)
