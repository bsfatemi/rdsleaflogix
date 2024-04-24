box::use(
  hcadispense[get_carts],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
apikey <- args$apikey
venues_ids <- args$venues_ids

start_utc <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))
end_utc <- today("UTC")

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/dispense/", org, "_", store), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
if (is.null(venues_ids)) {
  json <- get_carts(apikey, start_utc, end_utc)
} else {
  json <- unlist(lapply(venues_ids, function(venue_id) {
    get_carts(apikey, start_utc, end_utc, venue_id)
  }))
}

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "carts", paste(org, store, sep = "_"), "dispense")
