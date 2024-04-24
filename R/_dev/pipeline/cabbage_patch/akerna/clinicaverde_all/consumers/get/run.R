# VARS --------------------------------------------------------------------
org <- "clinicaverde"
uuid <- hcaconfig::lookupOrgGuid(org)
apikey <- hcaakerna::getApiKey(uuid)$apikey

# RUN ---------------------------------------------------------------------
js <- hcaakerna::get_akerna(config = apikey$all, item = "consumers", facility_id = "")
dir.create(paste0("/mnt/data/pipeline/raw/akerna/", org, "_all"), showWarnings = FALSE)
# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(js, "consumers", paste0(org, "_all"), "akerna")
