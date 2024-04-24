# VARS --------------------------------------------------------------------
org <- "windycity"
store <- "highwood"
end_date <- lubridate::floor_date(lubridate::now("America/Los_Angeles"), "day")
start_date <- end_date - lubridate::days(1)

# QUERY -------------------------------------------------------------------
json <- hcaiheartjane::get_partner_reservation_products(org, store, end_date, start_date)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "reservation_products", paste0(org, "_", store), "iheartjane")
