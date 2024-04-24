# VARS --------------------------------------------------------------------
uuid <- hcaconfig::lookupOrgGuid("amuse")
apikey <- hcatreez::getApiKey(uuid)$apikey$amuse_lux$apikey
client_id <- hcatreez::getHCAKey()
dispensary_name <- hcatreez::getApiKey(uuid)$apikey$amuse_lux$disp_name

ept <- "/customer"
eob_utc <- utils::URLencode(
  format(lubridate::today("UTC") - lubridate::days(1), "%Y-%m-%dT%H:%M:%S.000-07:00"),
  reserved = TRUE
)

endpt <- stringr::str_glue("{ept}/lastUpdated/after/{eob_utc}")
# QUERY -------------------------------------------------------------------

json <- hcatreez::getPageTreez(
  apikey = apikey,
  client_id = client_id,
  endpt = endpt,
  dispensary_name = dispensary_name
)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "customers", "amuse_lux", "treez2-clients")
