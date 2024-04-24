# VARS --------------------------------------------------------------------
org <- store <- "medithrive"
out_table <- paste0(org, "_iheartjane_reservations")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("reservations", org, "iheartjane")
reservations <- hcaiheartjane::extract_partner_reservations(json, org, store)

# LAND --------------------------------------------------------------------
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
reservations <- pipelinetools::check_cols(reservations, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, reservations, append = TRUE)
hcaconfig::dbd(pg)
