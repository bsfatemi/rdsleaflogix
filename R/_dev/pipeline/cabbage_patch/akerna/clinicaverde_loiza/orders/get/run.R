# VARS --------------------------------------------------------------------
org <- "clinicaverde"
store <- "loiza"
raw_dir <- paste(org, store, sep = "_")
uuid <- hcaconfig::lookupOrgGuid(org)
apikey <- hcaakerna::getApiKey(uuid)$apikey

eob_utc <- lubridate::today("UTC") - lubridate::days(1)

# RUN ---------------------------------------------------------------------
js <- hcaakerna::get_akerna(
  config = apikey$all, item = "orders", facility_id = apikey[[store]], completed_date_from = eob_utc
)
dir.create(paste0("/mnt/data/pipeline/raw/akerna/", raw_dir), showWarnings = FALSE)
# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(js, "orders", raw_dir, "akerna")
