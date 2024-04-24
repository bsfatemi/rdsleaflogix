# VARS --------------------------------------------------------------------
org <- "clinicaverde"
uuid <- hcaconfig::lookupOrgGuid(org)
apikey <- hcaakerna::getApiKey(uuid)$apikey

# RUN ---------------------------------------------------------------------
eob_utc <- format(
  lubridate::today("UTC") - lubridate::days(Sys.getenv("PREVIOUS_DAYS", 1)),
  "%Y-%m-%d %H:%M:%S"
)
json <- hcaakerna::get_akerna(
  config = apikey$all, item = "item_masters", facility_id = "",
  updated_at_from = eob_utc
)
dir.create(paste0("/mnt/data/pipeline/raw/akerna/", org, "_all"), showWarnings = FALSE)
# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "item_masters", paste0(org, "_all"), "akerna")
