# VARS --------------------------------------------------------------------
org <- "windycity"
store <- "litchfield_rec"
out_table <- paste(org, store, "iheartjane_reservations", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("reservations", paste0(org, "_", store), "iheartjane")
reservations <- hcaiheartjane::extract_partner_reservations(json, org, store)

# LAND --------------------------------------------------------------------
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
reservations <- pipelinetools::check_cols(reservations, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, reservations, append = TRUE)
hcaconfig::dbd(pg)
