# VARS ------------------------------------------------------------------
org <- "clinicaverde"
store <- "pinero"
raw_dir <- paste(org, store, sep = "_")
uuid <- hcaconfig::lookupOrgGuid(org)
apikey <- hcaakerna::getApiKey(uuid)$apikey

# RUN ---------------------------------------------------------------------
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
order_ids <- DBI::dbGetQuery(pg, stringr::str_glue(
  "select distinct id as order_id, name from {org}_{store}_akerna_orders"
))
queried <- DBI::dbGetQuery(pg, stringr::str_glue(
  "select distinct id, name from {org}_{store}_akerna_orders_products"
))
hcaconfig::dbd(pg)
order_ids <- order_ids[which(!order_ids$name %in% queried$name), ]
json <- hcaakerna::get_akerna_order_details(
  config = apikey$all, order_ids = order_ids$order_id, facility_id = apikey[[store]]
)
dir.create(paste0("/mnt/data/pipeline/raw/akerna/", raw_dir), showWarnings = FALSE)
# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "order_details", raw_dir, "akerna")
