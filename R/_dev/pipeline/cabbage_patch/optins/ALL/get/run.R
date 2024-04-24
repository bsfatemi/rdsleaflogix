# Collect Opt-Ins
# run.R
#
# (C) Happy Cabbage Analytics 2021

# QUERY
optins <- optintools::collectForms()
optins <- dplyr::mutate(
  optins, "optin_id" = purrr::map_chr(dplyr::row_number(), uuid::UUIDgenerate)
) ## FOR REFERENCE LATER

# STORE
pipelinetools::storeExternalRaw(
  optins, name = "optins", clientName = "ALL", dSource = "hca", rawExt_d = "/mnt/data/pipeline/raw/"
)
