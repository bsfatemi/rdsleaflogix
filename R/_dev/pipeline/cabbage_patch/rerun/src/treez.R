# Save the old WD, to restore it at the end.
old_wd <- getwd()
# If we are under hcapipelines path, keep it. If not, move to cypress default path.
if (basename(getwd()) != "hcapipelines") {
  setwd("~/hcapipelines")
}
source("inst/cron/run_and_log.R")
wd <- "./"

box::use(
  dplyr[anti_join, bind_cols, collect, count, distinct, filter, mutate, tbl, transmute, tribble],
  hcaconfig[dbc, dbd, orgIndex],
  hcatreez[get_api_keys, getHCAKey],
  lubridate[as_date, today, weeks],
  purrr[map_dfr, transpose, walk]
)

### Get pipelines to re-run.

org <- orgIndex()[curr_client == TRUE]
seq_dates <- data.frame(date = today("UTC"))

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")

# treez.
treez_creds <- get_api_keys() |>
  filter(org_uuid %in% org$org_uuid) |>
  transmute(
    org = org_short_name, store = store_short_name, uuid = org_uuid,
    dispensary_name = dispensary_name, apikey = apikey, client_id = getHCAKey()
  )

# List of tables to fill, and the endpoint that fills it. Only endpoint that query by date.
endpoints <- tribble(
  ~ table,            ~ pipeline,
  "customers",        "customers",
  "products",         "products",
  "ticket_discounts", "tickets",
  "ticket_items",     "tickets",
  "ticket_payments",  "tickets",
  "ticket_taxes",     "tickets",
  "tickets",          "tickets"
) |>
  mutate(pipeline = paste0("inst/pipeline/cabbage_patch/treez2-clients/main/", pipeline))

# For each credential:
missing_dates <- map_dfr(transpose(treez_creds), function(creds) {
  message(creds$org, " - ", creds$store)
  missing_dates <- map_dfr(transpose(endpoints), function(endpoint) {
    # Obtain number of rows present per day.
    data_dates <- tbl(pg, paste(creds$org, creds$store, "trz2", endpoint$table, sep = "_")) |>
      filter(run_date_utc >= !!min(seq_dates$date)) |>
      count(date = as_date(run_date_utc)) |>
      collect()
    # Check if we are missing rows for `seq_dates`.
    missing_dates <- anti_join(seq_dates, data_dates, by = "date")
    if (nrow(missing_dates) > 0) {
      # If missing dates, then return the required structure for re-running.
      bind_cols(creds, endpoint, missing_dates)
    }
  })
})
# De-duplicate by pipeline (as there are pipelines that fill multiple tables).
if (nrow(missing_dates) > 0) {
  missing_dates <- distinct(missing_dates, org, store, pipeline, date, .keep_all = TRUE)
}

dbd(pg)

### Re-run pipelines.

walk(transpose(missing_dates), function(args) {
  # Create raw data storage dir if it does not exist.
  dir.create(
    paste0("/mnt/data/pipeline/raw/treez2-clients/", args$org, "_", args$store),
    showWarnings = FALSE
  )
  process <- gsub(".*/", "", args$pipeline)
  # For each endpoint to re-run, do it for get and extract steps.
  walk(c("get", "extract"), function(step) {
    run_and_log(
      paste("TREEZ RERUN:", args$org, args$store, step, process),
      wd,
      paste0(args$pipeline, "/", step, "/run.R"),
      args = args
    )
    Sys.sleep(1)
  })
})

# Restore working directory.
setwd(old_wd)
