# READ --------------------------------------------------------------------
source("inst/pipeline/cabbage_patch/treez2-clients/amuse_lux/tickets/extract/init.R")
js <- pipelinetools::rd_raw_archive("tickets", "amuse_lux", "treez2-clients")

# EXTRACT -----------------------------------------------------------------
tickets <- hcatreez::extractTreez(js, "tickets")
foh_tickets <- list()
# DIVERGES HERE --FRONT OF HOUSE

foh_tickets$ticket_items <- dplyr::filter(tickets$ticket_items, location_name == "FRONT OF HOUSE")
foh_ids <- foh_tickets$ticket_items$ticket_id

foh_tickets$tickets <- tickets$tickets %>% dplyr::filter(ticket_id %in% foh_ids)
foh_tickets$ticket_discounts <- tickets$ticket_discounts %>% dplyr::filter(ticket_id %in% foh_ids)
foh_tickets$ticket_payments <- tickets$ticket_payments %>% dplyr::filter(ticket_id %in% foh_ids)
foh_tickets$ticket_taxes <- tickets$ticket_taxes %>% dplyr::filter(ticket_id %in% foh_ids)

foh_tickets$ticket_discounts$run_date_utc <- lubridate::now("UTC")
foh_tickets$ticket_items$run_date_utc <- lubridate::now("UTC")
foh_tickets$ticket_payments$run_date_utc <- lubridate::now("UTC")
foh_tickets$ticket_taxes$run_date_utc <- lubridate::now("UTC")
foh_tickets$tickets$run_date_utc <- lubridate::now("UTC")

# TICKETS WE WANT
tickets$ticket_items <- tickets$ticket_items %>% dplyr::filter(location_name != "FRONT OF HOUSE")
ids <- tickets$ticket_items$ticket_id

tickets$tickets <- tickets$tickets %>% dplyr::filter(ticket_id %in% ids)
tickets$ticket_discounts <- tickets$ticket_discounts %>% dplyr::filter(ticket_id %in% ids)
tickets$ticket_payments <- tickets$ticket_payments %>% dplyr::filter(ticket_id %in% ids)
tickets$ticket_taxes <- tickets$ticket_taxes %>% dplyr::filter(ticket_id %in% ids)


tickets$ticket_discounts$run_date_utc <- lubridate::now("UTC")
tickets$ticket_items$run_date_utc <- lubridate::now("UTC")
tickets$ticket_payments$run_date_utc <- lubridate::now("UTC")
tickets$ticket_taxes$run_date_utc <- lubridate::now("UTC")
tickets$tickets$run_date_utc <- lubridate::now("UTC")

# WRITE TO DB -------------------------------------------------------------

pg <- hcaconfig::dbc("prod2", "cabbage_patch")


ex <- DBI::dbListFields(pg, "lux_foh_trz2_tickets")
final <- hcablaze::chkBlzCols(foh_tickets$tickets, ex)
DBI::dbWriteTable(pg, "lux_foh_trz2_tickets", final, append = TRUE)

ex <- DBI::dbListFields(pg, "lux_foh_trz2_ticket_items")
final <- hcablaze::chkBlzCols(foh_tickets$ticket_items, ex)
DBI::dbWriteTable(pg, "lux_foh_trz2_ticket_items", final, append = TRUE)

ex <- DBI::dbListFields(pg, "lux_foh_trz2_ticket_payments")
final <- hcablaze::chkBlzCols(foh_tickets$ticket_payments, ex)
DBI::dbWriteTable(pg, "lux_foh_trz2_ticket_payments", final, append = TRUE)

ex <- DBI::dbListFields(pg, "lux_foh_trz2_ticket_taxes")
final <- hcablaze::chkBlzCols(foh_tickets$ticket_taxes, ex)
DBI::dbWriteTable(pg, "lux_foh_trz2_ticket_taxes", final, append = TRUE)

ex <- DBI::dbListFields(pg, "lux_foh_trz2_ticket_discounts")
final <- hcablaze::chkBlzCols(foh_tickets$ticket_discounts, ex)
DBI::dbWriteTable(pg, "lux_foh_trz2_ticket_discounts", final, append = TRUE)



ex <- DBI::dbListFields(pg, "amuse_lux_trz2_tickets")
final <- hcablaze::chkBlzCols(tickets$tickets, ex)
DBI::dbWriteTable(pg, "amuse_lux_trz2_tickets", final, append = TRUE)

ex <- DBI::dbListFields(pg, "amuse_lux_trz2_ticket_items")
final <- hcablaze::chkBlzCols(tickets$ticket_items, ex)
DBI::dbWriteTable(pg, "amuse_lux_trz2_ticket_items", final, append = TRUE)

ex <- DBI::dbListFields(pg, "amuse_lux_trz2_ticket_payments")
final <- hcablaze::chkBlzCols(tickets$ticket_payments, ex)
DBI::dbWriteTable(pg, "amuse_lux_trz2_ticket_payments", final, append = TRUE)

ex <- DBI::dbListFields(pg, "amuse_lux_trz2_ticket_taxes")
final <- hcablaze::chkBlzCols(tickets$ticket_taxes, ex)
DBI::dbWriteTable(pg, "amuse_lux_trz2_ticket_taxes", final, append = TRUE)

ex <- DBI::dbListFields(pg, "amuse_lux_trz2_ticket_discounts")
final <- hcablaze::chkBlzCols(tickets$ticket_discounts, ex)
DBI::dbWriteTable(pg, "amuse_lux_trz2_ticket_discounts", final, append = TRUE)
hcaconfig::dbd(pg)
