# VARS --------------------------------------------------------------------
org <- "windycity"
store <- "highwood"
out_table <- paste(org, store, "iheartjane_reservation_products", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("reservation_products", paste0(org, "_", store), "iheartjane")
reservation_products <- hcaiheartjane::extract_partner_reservation_products(json, org, store)

# LAND --------------------------------------------------------------------
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
reservation_products <- pipelinetools::check_cols(
  reservation_products, DBI::dbListFields(pg, out_table)
)
DBI::dbWriteTable(pg, out_table, reservation_products, append = TRUE)
hcaconfig::dbd(pg)
