# VARS --------------------------------------------------------------------
org <- store <- "medithrive"
out_table <- paste0(org, "_iheartjane_reservation_products")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("reservation_products", org, "iheartjane")
reservation_products <- hcaiheartjane::extract_partner_reservation_products(json, org, store)

# LAND --------------------------------------------------------------------
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
reservation_products <- pipelinetools::check_cols(
  reservation_products, DBI::dbListFields(pg, out_table)
)
DBI::dbWriteTable(pg, out_table, reservation_products, append = TRUE)
hcaconfig::dbd(pg)
