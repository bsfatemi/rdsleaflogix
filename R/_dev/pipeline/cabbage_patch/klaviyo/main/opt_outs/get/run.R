# VARS --------------------------------------------------------------------
org <- args$org
uuid <- args$uuid
apikey <- args$apikey

# RUN ---------------------------------------------------------------------
json <- hcaklaviyo::get_global_exclusions_and_unsubscribes(apikey)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "opt_outs", org, "klaviyo")
