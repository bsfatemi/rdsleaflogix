# Variables --------------------------------------------------------------------
org <- args$org
store <- args$store
uuid <- args$uuid
company_id <- args$company_id
client_id <- args$client_id
token <- args$token


endpt <- "people"
start <- as.character(lubridate::today() - lubridate::days(Sys.getenv("PREVIOUS_DAYS", 1)))
end <- as.character(lubridate::today())
date_range <- paste0(start, ",", end)

# Query -------------------------------------------------------------------
json <- hcagreenbits::get_gb(endpt, company_id, client_id, token,
  "by_created_at[comparator]" = "between",
  "by_created_at[date]" = date_range
)


# Archive -----------------------------------------------------------------

pipelinetools::storeExternalRaw(json, "customers", paste(org, store, sep = "_"), "greenbits")
