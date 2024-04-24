# VARS --------------------------------------------------------------------
org <- "jarsaz"
store <- "payson_rec"
end_date <- lubridate::floor_date(lubridate::now("America/Los_Angeles"), "day")
start_date <- end_date - lubridate::days(1)

# QUERY -------------------------------------------------------------------
json <- hcaiheartjane::get_partner_reservations(org, store, end_date, start_date)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "reservations", paste0(org, "_", store), "iheartjane")
