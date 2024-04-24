




# INDX <- rdleaflogix:::.pos_data$leaflogix$extract_index
#
# stock <- build_ll_inventory(r$data)
#
# stock_by_room     <- stock$rooms_stock
# stock_lab_results <- stock$lab_results
# stock_snapshot    <- stock$store_stock
#
# customer_summary <- build_ll_customers(r$data)   # customerId lastModifiedDateUTC
# customer_loyalty <- build_ll_loyalty(r$data)     # customerId
# store_employees  <- build_ll_employees(r$data)   # userId loginId
# order_items      <- build_ll_order_items(r$data) # transactionId transactionItemId productId
# order_summary    <- build_ll_orders(r$data)      # transactionId customerId employeeId
# brand_index      <- build_ll_brands(r$data)      # brandId
# product_summary  <- build_ll_products(r$data)    # productId sku
#
# setnames(stock_by_room,     stringr::str_to_lower(names(stock_by_room)))
# setnames(stock_lab_results, stringr::str_to_lower(names(stock_lab_results)))
# setnames(stock_snapshot,    stringr::str_to_lower(names(stock_snapshot)))
# setnames(customer_summary,  stringr::str_to_lower(names(customer_summary)))
# setnames(customer_loyalty,  stringr::str_to_lower(names(customer_loyalty)))
# setnames(store_employees,   stringr::str_to_lower(names(store_employees)))
# setnames(order_items,       stringr::str_to_lower(names(order_items)))
# setnames(order_summary,     stringr::str_to_lower(names(order_summary)))
# setnames(brand_index,       stringr::str_to_lower(names(brand_index)))
# setnames(product_summary,   stringr::str_to_lower(names(product_summary)))
#
#
# db_append_consolidated <- function(pl, tb, .data) {
#   cn <- dbc("dev2", "consolidated")
#   on.exit(dbd(cn))
#   tbnam <- Id(schema = pl, table = tb)
#   qry <- sqlInterpolate(cn, "SELECT * FROM ?x", x = dbQuoteIdentifier(cn, tbnam))
#   res <- dbSendQuery(cn, qry)
#   on.exit(dbClearResult(res), add = TRUE, after = FALSE)
#   d0 <- setDT(dbFetch(res, 0))
#   convert_cols <- names(which(unlist(sapply(d0, function(x) class(x) == "pq_json"))))
#   for (col in convert_cols)
#     d0[, data.table::setattr(get(col), "class", "json")]
#   DT <- rbindlist(list(d0, .data), use.names = TRUE)
#   x <- dbAppendTable(cn, tbnam, DT)
#   log_suc(str_glue("(Append) +{scales::comma(x)} rows [consolidated].[{pl}].[{tb}]"))
#   invisible(x)
# }
#
#
# ##
# ## stock_by_room
# ##
# ckeys <- c("org_uuid", "store_uuid", "productid", "sku", "packageid", "roomid")
# setkeyv(stock_by_room, ckeys)
#
# no_na <- c(ckeys, "lastmodifieddateutc")
# ss_data <- na.omit(stock_by_room, no_na)[, .SD[which.max(lastmodifieddateutc)], ckeys]
#
# db_append_consolidated("leaflogix", "stock_by_room", ss_data)
#
# ##
# ## stock_lab_results
# ##
# ckeys <- c("org_uuid", "store_uuid", "productid", "sku", "packageid", "labtest")
# setkeyv(stock_lab_results, ckeys)
#
# no_na <- c(ckeys, "lastmodifieddateutc")
# ss_data <- na.omit(stock_lab_results, no_na)[, .SD[which.max(lastmodifieddateutc)], ckeys]
#
# db_append_consolidated("leaflogix", "stock_lab_results", ss_data)
#
# ##
# ## stock_snapshot
# ##
# ckeys <- c("org_uuid", "store_uuid", "productid", "sku", "packageid")
# setkeyv(stock_snapshot, ckeys)
#
# no_na <- c(ckeys, "lastmodifieddateutc")
# ss_data <- na.omit(stock_snapshot, no_na)[, .SD[which.max(lastmodifieddateutc)], ckeys]
#
# db_append_consolidated("leaflogix", "stock_snapshot", ss_data)
#
#
# ##
# ## customer_summary
# ##
# ckeys <- c("org_uuid", "store_uuid", "customerid")
# setkeyv(customer_summary, ckeys)
#
# no_na <- c(ckeys, "lastmodifieddateutc")
# ss_data <- na.omit(customer_summary, no_na)[, .SD[which.max(lastmodifieddateutc)], ckeys]
#
# ss_data[stringr::str_length(phone) > 20, phone := NA]
# ss_data[stringr::str_length(cellphone) > 20, cellphone := NA]
#
# ss_data[, name := stringr::str_trunc(name, 50)]
# ss_data[, firstname := stringr::str_trunc(firstname, 50)]
# ss_data[, lastname := stringr::str_trunc(lastname, 50)]
# ss_data[, nameprefix := stringr::str_trunc(nameprefix, 50)]
# ss_data[, namesuffix := stringr::str_trunc(namesuffix, 50)]
#
# db_append_consolidated("leaflogix", "customer_summary", ss_data)
#
#
# ##
# ## customer_loyalty
# ##
# ckeys <- c("org_uuid", "store_uuid", "customerid")
# setkeyv(customer_loyalty, ckeys)
#
# ss_data <- na.omit(customer_loyalty, ckeys)
#
# db_append_consolidated("leaflogix", "customer_loyalty", ss_data)
#
#
#
# ##
# ## brand_index
# ##
# ckeys <- c("org_uuid", "store_uuid", "brandid")
# setkeyv(brand_index, ckeys)
#
# ss_data <- na.omit(brand_index, names(brand_index))
#
# db_append_consolidated("leaflogix", "brand_index", ss_data)
#
#
#
# ##
# ## store_employees
# ##
# ckeys <- c("org_uuid", "store_uuid", "userid", "permissionslocation")
# setkeyv(store_employees, ckeys)
#
# store_employees[, mmjexpiration := as_date(mmjexpiration)]
#
# ss_data <- unique(na.omit(store_employees, c(ckeys, "org", "store", "loginid")))
#
# db_append_consolidated("leaflogix", "store_employees", ss_data)
#
#
#
# ##
# ## order_items
# ##
# ckeys <- c("org_uuid", "store_uuid", "transactionid", "transactionitemid", "productid", "packageid")
# setkeyv(order_items, ckeys)
#
# ss_data <- unique(na.omit(order_items, c(ckeys, "org", "store")))
#
# ss_data[, .N, ckeys][order(-N)]
# db_append_consolidated("leaflogix", "order_items", ss_data)
#
#
# ##
# ## order_summary
# ##
# ckeys <- c("org_uuid", "store_uuid", "transactionid", "customerid", "employeeid")
# setkeyv(order_summary, ckeys)
#
# ss_data <- unique(na.omit(order_summary, c(ckeys, "org", "store", "transactiondate")))
#
# ss_data[, esttimearrivallocal := lubridate::as_datetime(esttimearrivallocal)]
# db_append_consolidated("leaflogix", "order_summary", ss_data)
#
#
# ##
# ## product_summary
# ##
#
# ckeys <- c("org_uuid", "store_uuid", "productid", "sku")
# setkeyv(product_summary, ckeys)
#
# tmp <- unique(na.omit(product_summary, c(ckeys, "org", "store", "lastmodifieddateutc")))
# ss_data <- product_summary[, .SD[which.max(lastmodifieddateutc)], ckeys]
#
# db_append_consolidated("leaflogix", "product_summary", ss_data)
#
#
#
#
#
#
#
#
#
#
#
