# VARS ------------------------------------------------------------------
org <- "clinicaverde"
store <- "pinero"
raw_dir <- paste(org, store, sep = "_")
json <- pipelinetools::rd_raw_archive("order_details", raw_dir, "akerna")

# RUN ---------------------------------------------------------------------
ord_details <- hcaakerna::extract_akerna_order_details(json)
ord_details <- purrr::map(ord_details, dplyr::mutate, run_date_utc = lubridate::now(tzone = "UTC"))
products <- ord_details$products
payments <- ord_details$payments
coupons <- ord_details$coupons
taxes <- ord_details$taxes

# ARCHIVE -------------------------------------------------------------------

# Products
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
ex <- DBI::dbListFields(pg, paste0(raw_dir, "_akerna_orders_products"))
products <- pipelinetools::check_cols(products, ex)
DBI::dbWriteTable(pg, paste0(raw_dir, "_akerna_orders_products"), products, append = TRUE)
hcaconfig::dbd(pg)

# Payments
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
ex <- DBI::dbListFields(pg, paste0(raw_dir, "_akerna_orders_payments"))
payments <- pipelinetools::check_cols(payments, ex)
DBI::dbWriteTable(pg, paste0(raw_dir, "_akerna_orders_payments"), payments, append = TRUE)
hcaconfig::dbd(pg)

# Coupons
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
ex <- DBI::dbListFields(pg, paste0(raw_dir, "_akerna_orders_coupons"))
coupons <- pipelinetools::check_cols(coupons, ex)
DBI::dbWriteTable(pg, paste0(raw_dir, "_akerna_orders_coupons"), coupons, append = TRUE)
hcaconfig::dbd(pg)

# Taxes
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
ex <- DBI::dbListFields(pg, paste0(raw_dir, "_akerna_orders_taxes"))
taxes <- pipelinetools::check_cols(taxes, ex)
DBI::dbWriteTable(pg, paste0(raw_dir, "_akerna_orders_taxes"), taxes, append = TRUE)
hcaconfig::dbd(pg)
