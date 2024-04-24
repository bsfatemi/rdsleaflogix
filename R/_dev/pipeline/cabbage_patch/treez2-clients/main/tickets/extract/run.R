# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "trz2_tickets", sep = "_")
out_table_ti <- paste(org, store, "trz2_ticket_items", sep = "_")
out_table_tp <- paste(org, store, "trz2_ticket_payments", sep = "_")
out_table_tt <- paste(org, store, "trz2_ticket_taxes", sep = "_")
out_table_td <- paste(org, store, "trz2_ticket_discounts", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("tickets", paste(org, store, sep = "_"), "treez2-clients")

# EXTRACT -----------------------------------------------------------------
now_utc <- lubridate::now(tzone = "UTC")
tickets <- hcatreez::extractTreez(json, "tickets")
ticket_items <- dplyr::mutate(tickets$ticket_items, run_date_utc = now_utc)
ticket_payments <- dplyr::mutate(tickets$ticket_payments, run_date_utc = now_utc)
ticket_taxes <- dplyr::mutate(tickets$ticket_taxes, run_date_utc = now_utc)
ticket_discounts <- dplyr::mutate(tickets$ticket_discounts, run_date_utc = now_utc)
tickets <- dplyr::mutate(tickets$tickets, run_date_utc = now_utc)

# LAND --------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
tickets <- pipelinetools::check_cols(tickets, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, tickets, append = TRUE)
ticket_items <- pipelinetools::check_cols(ticket_items, DBI::dbListFields(pg, out_table_ti))
DBI::dbWriteTable(pg, out_table_ti, ticket_items, append = TRUE)
ticket_payments <- pipelinetools::check_cols(ticket_payments, DBI::dbListFields(pg, out_table_tp))
DBI::dbWriteTable(pg, out_table_tp, ticket_payments, append = TRUE)
ticket_taxes <- pipelinetools::check_cols(ticket_taxes, DBI::dbListFields(pg, out_table_tt))
DBI::dbWriteTable(pg, out_table_tt, ticket_taxes, append = TRUE)
ticket_discounts <- pipelinetools::check_cols(ticket_discounts, DBI::dbListFields(pg, out_table_td))
DBI::dbWriteTable(pg, out_table_td, ticket_discounts, append = TRUE)
hcaconfig::dbd(pg)
