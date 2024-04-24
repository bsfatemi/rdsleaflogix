# VARS --------------------------------------------------------------------
org <- "amuse"
store_short <- "lux"
uuid <- hcaconfig::lookupOrgGuid(org)
apikey <- hcatreez::getApiKey(uuid)$apikey[[paste(org, store_short, sep = "_")]]

client_id <- hcatreez::getHCAKey()
dispensary_name <- apikey$disp_name
apikey <- apikey$apikey
ept <- "/ticket/closedate"

# Query for this exact date.
eob_utc <- format(lubridate::today("UTC") - lubridate::days(1), "%Y-%m-%d")
endpt <- stringr::str_glue("{ept}/{eob_utc}")

# QUERY -------------------------------------------------------------------
json <- hcatreez::getPageTreez(apikey, client_id, endpt, dispensary_name)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(
  json, "tickets", paste(org, store_short, sep = "_"), "treez2-clients"
)
