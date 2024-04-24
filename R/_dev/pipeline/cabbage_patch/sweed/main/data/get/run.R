box::use(
  hcasweed[get_sweed_data],
  lubridate[as_datetime, days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
uuid <- args$uuid
apikey <- args$apikey

start_utc <- as_datetime(today("UTC")) - days(Sys.getenv("PREVIOUS_DAYS", 1))
stop_utc <- as_datetime(today("UTC"))

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/sweed/", org), showWarnings = FALSE)

# RUN ---------------------------------------------------------------------
json <- get_sweed_data(start_utc, stop_utc, apikey)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "data", org, "sweed")

# ERROR HANDLING ----------------------------------------------------------
if (!is.null(attr(json, "error"))) {
  stop(attr(json, "error"))
}
