box::use(
  hcatreez[getHCAKey, getProducts],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)
# VARS --------------------------------------------------------------------
org <- "amuse"
dispensary_name <- facility <- "lux"
org_uuid <- "f0d2060d-9f1d-4d66-93d4-f106638aea91"
eob_utc <- today() - days(Sys.getenv("PREVIOUS_DAYS", 1))
apikey <- "MmY2OTI1MzdkYTkzMTlhMmJlN"
hca <- getHCAKey()
# QUERY -------------------------------------------------------------------
json <- getProducts(apikey, hca, dispensary_name, eob_utc)
# STORE -------------------------------------------------------------------
storeExternalRaw(
  data = json,
  name = "products",
  clientName = paste0(org, "_", facility),
  dSource = "treez2-clients",
  rawExt_d = "/mnt/data/pipeline/raw"
)
