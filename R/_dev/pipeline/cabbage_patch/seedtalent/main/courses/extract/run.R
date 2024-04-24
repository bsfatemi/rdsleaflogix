box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcaseedtalent[extract_courses],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
integrator_id <- args$integrator_id
out_table <- "seedtalent_courses"

# READ --------------------------------------------------------------------
json_files <- rd_raw_archive("courses", paste("all", integrator_id, sep = "_"), "seedtalent")

# EXTRACT -----------------------------------------------------------------
courses <- extract_courses(json_files) |>
  mutate(
    integrator_id = integrator_id,
    run_date_utc = now("UTC")
  )

# LAND --------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
try(courses <- check_cols(courses, dbListFields(pg, out_table)))
dbWriteTable(pg, out_table, courses, append = TRUE)
dbd(pg)
