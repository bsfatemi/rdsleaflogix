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
  hcaleaflogix[get_api_keys],
  lubridate[as_date, today, weeks],
  purrr[map_dfr, transpose, walk]
)

### Get pipelines to re-run.

org <- orgIndex()[curr_client == TRUE]
seq_dates <- data.frame(date = today("UTC"))

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")

# LeafLogix.
leaflogix_creds <- get_api_keys() |>
  filter(org_uuid %in% org$org_uuid) |>
  transmute(
    org = org_short_name, store = store_short_name, uuid = org_uuid, consumerkey = consumerkey,
    auth = auth
  )

# List of tables to fill, and the endpoint that fills it. Only endpoint that query by date.
endpoints <- tribble(
  ~ table,                  ~ pipeline,
  "customers",            "customers",
  # "employees",            "employees",
  # "inventory",            "inventory",
  "line_items",           "transactions",
  "line_items_discounts", "transactions",
  "line_items_taxes",     "transactions",
  # "loyalty",              "loyalty",
  # "product_category",     "product_category",
  "products",             "products",
  # "room_quantities",      "inventory",
  # "strains",              "strains",
  "transactions",         "transactions"
) |>
  mutate(pipeline = paste0("inst/pipeline/cabbage_patch/leaflogix/main/", pipeline))

# For each credential:
missing_dates <- map_dfr(transpose(leaflogix_creds), function(creds) {
  message(creds$org, " - ", creds$store)
  missing_dates <- map_dfr(transpose(endpoints), function(endpoint) {
    # Obtain number of rows present per day.
    data_dates <- tbl(pg, paste(creds$org, creds$store, "ll", endpoint$table, sep = "_")) |>
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
    paste0("/mnt/data/pipeline/raw/leaflogix/", args$org, "_", args$store), showWarnings = FALSE
  )
  process <- gsub(".*/", "", args$pipeline)
  # For each endpoint to re-run, do it for get and extract steps.
  walk(c("get", "extract"), function(step) {
    run_and_log(
      paste("LEAFLOGIX RERUN:", args$org, args$store, step, process),
      wd,
      paste0(args$pipeline, "/", step, "/run.R"),
      args = args
    )
    Sys.sleep(1)
  })
})

# Restore working directory.
setwd(old_wd)
