#' Build leaflogix consolidated
#'
#' @param index output list from extracted step
#' @param org optional org for logging
#' @param store optional store for logging
#'
#' @import data.table
#' @importFrom lubridate as_datetime as_date
#' @importFrom stringr str_remove_all str_to_upper str_to_lower str_subset str_flatten_comma
#' str_glue
#' @importFrom jsonlite toJSON
#' @importFrom RPostgres Postgres
#'
#' @return Returns a consolidated table for each endpoint that contains all leaflogix locations
#'
#' @name leaflogix-build
NULL


#' @describeIn leaflogix-build  build population
#' @export
bld_ll_population <- function(index, org, store) {
  lmsg <- stringr::str_glue("Leaflogix|BLD|population|{org}.{store}")

  if (is.null(index)) {
    log_err(paste0(lmsg, "|...no extracted data"))
    return(list(population = NULL))
  }

  cons_tabs <- list(
    "order_summary" = index[name == "order_summary", rbindlist(data)],
    "order_items" = index[name == "order_items", rbindlist(data)],
    "product_summary" = index[name == "product_summary", rbindlist(data)],
    "customer_summary" = index[name == "customer_summary", rbindlist(data)]
  )

  log_inf(paste0(lmsg, "|...Checking input tables for pop build"))
  for (tb in names(cons_tabs)) {
    if (nrow(cons_tabs[[tb]]) == 0) {
      log_err(paste0(lmsg, "|...No rows in required table"), tb)
      return(list(population = NULL))
    }
  }

  ##
  ## Customers
  ##
  log_inf(paste0(lmsg, "|...Cleaning customers"))
  customers <- copy(cons_tabs$customer_summary)[, .(
    org_uuid,
    store_uuid,
    org,
    store,
    customer_id = as.integer(customerId),
    customer_created_at_utc = creationDate,
    customer_last_updated_utc = lastModifiedDateUTC,
    first_name = firstName,
    last_name = lastName,
    phone = cellPhone,
    email = emailAddress,
    user_type = customerType,
    gender = gender,
    birthday = dateOfBirth,
    has_loyalty = isLoyaltyMember,
    customer_address_street1 = address1,
    customer_address_street2 = address2,
    customer_city = city,
    customer_state = state,
    customer_zipcode = postalCode
  )]

  ##
  ## Orders
  ##
  log_inf(paste0(lmsg, "|...Cleaning orders"))
  orders <- copy(cons_tabs$order_summary)[(isReturn == FALSE), .(
    org_uuid,
    store_uuid,
    org,
    store,
    order_id = as.integer(transactionId),
    customer_id = as.integer(customerId),
    sold_by_id = as.integer(employeeId),
    sold_by = completedByUser,
    order_type = orderType,
    order_source = orderSource,
    order_method = orderMethod,
    was_preorder = wasPreOrdered,
    checkin_time_utc = checkInDate,
    order_time_utc = transactionDate,
    order_time_local = transactionDateLocalTime,
    arrival_time_local = estTimeArrivalLocal,
    deliver_time_local = estDeliveryDateLocal,
    is_medical = isMedical,
    transact_type = transactionType,
    order_subtotal = subtotal,
    order_discount = totalDiscount,
    order_total_pretax = totalBeforeTax,
    order_tax = tax,
    order_total = total,
    total_paid = paid,
    total_change = changeDue,
    order_qty = totalItems,
    loyalty_spent = loyaltySpent,
    loyalty_earned = loyaltyEarned
  )][ is.na(loyalty_spent), loyalty_spent := 0][
    is.na(loyalty_earned), loyalty_earned := 0][]

  ##
  ## Order Items
  ##
  log_inf(paste0(lmsg, "|...Cleaning order items"))
  items <- copy(cons_tabs$order_items)[
    !is.na(order_item_tax_json),
    totalTax := sum(jsonlite::fromJSON(order_item_tax_json)$amount),
    .(org_uuid, store_uuid, transactionItemId)
  ][(isReturned == FALSE), .(
    org_uuid,
    store_uuid,
    org,
    store,
    order_id = as.integer(transactionId),
    order_line_id = as.integer(transactionItemId),
    product_id = as.integer(productId),
    pkg_id = packageId,
    src_pkg_id = sourcePackageId,
    product_qty = quantity,
    product_uom = unitWeightUnit,
    product_unit_count = unitWeight,
    price_per_qty = unitPrice,
    cost_per_qty = unitCost,
    order_line_total = totalPrice,
    order_line_discount = totalDiscount
  )]

  ##
  ## Products
  ##
  log_inf(paste0(lmsg, "|...Cleaning products"))
  products <- copy(cons_tabs$product_summary)[
    is.na(brandId) & !is.na(vendorId) & is.na(brandName),
    c("brandId", "brandName") := .(vendorId, vendorName)
  ][, .(
    org_uuid,
    store_uuid,
    org,
    store,
    product_id = as.integer(productId),
    product_sku = sku,
    product_name = productName,
    product_category_name = masterCategory,
    raw_category_name = category,
    product_class = strainType,
    product_strain = strain,
    brand_id = as.integer(brandId),
    brand_name = brandName,
    product_grams = productGrams,
    thc = thcContent,
    thc_unit = thcContentUnit,
    cbd = cbdContent,
    cbd_unit = cbdContentUnit
  )]

  ##
  ## Join Tables
  ##
  log_inf(paste0(lmsg, "|...Merging tables"))
  keycols <- c("org_uuid", "store_uuid", "org", "store")

  setkeyv(customers, c(keycols, "customer_id"))
  setkeyv(orders,    c(keycols, "order_id"))
  setkeyv(items,     c(keycols, "order_id", "order_line_id"))
  setkeyv(products,  c(keycols, "product_id"))

  ## merge orders and items
  log_inf(paste0(lmsg, "|...Join orders and order items"))
  tmp_data <- orders[items]

  ## merge in customers
  log_inf(paste0(lmsg, "|...Merge in customers"))
  setkeyv(tmp_data, c(keycols, "customer_id"))
  tmp_data <- customers[tmp_data]

  ## append product information
  log_inf(paste0(lmsg, "|...Append product information"))
  setkeyv(tmp_data, c(keycols, "product_id"))

  keepCols <- c("product_name", "product_sku", "product_category_name",
                "raw_category_name", "product_class", "product_strain",
                "brand_id", "brand_name", "product_grams")

  tmp_data[unique(products), (keepCols) := .(
    product_name, product_sku, product_category_name, raw_category_name,
    product_class, product_strain, brand_id, brand_name, product_grams
  )]

  ## Category
  log_inf(paste0(lmsg, "|...Creating category3"))
  set_category3(tmp_data)

  ## Brand
  log_inf(paste0(lmsg, "|...Standardizing brands"))
  set_brands(tmp_data)

  tmp_data[is.na(gender), gender := "U"]

  msg <- stringr::str_glue("|...Population complete with {nrow(tmp_data)} rows")
  log_suc(paste0(lmsg, msg))

  list(population = tmp_data[])
}


#' @describeIn leaflogix-build transactions
#' @export
bld_ll_transactions <- function(index, org, store) {
  list(
    "order_summary" = ..bld_ll_order_summary(index, org, store),
    "order_items" = ..bld_ll_order_items(index, org, store)
  )
}


#' @describeIn leaflogix-build stock snapshot
#' @export
bld_ll_stocksnapshot <- function(index, org, store) {
  list("stock_snapshot" = ..bld_ll_stock_snapshot(index, org, store))
}


#' @describeIn leaflogix-build inventory
#' @export
bld_ll_inventory <- function(index, org, store) {
  list(
    "stock_summary" = ..bld_ll_stock_summary(index, org, store),
    "stock_by_room" = ..bld_ll_stock_by_room(index, org, store),
    "stock_summary" = ..bld_ll_stock_summary(index, org, store)
  )
}


#' @describeIn leaflogix-build products
#' @export
bld_ll_products <- function(index, org, store) {
  list("product_summary" = ..bld_ll_product_summary(index, org, store))
}


#' @describeIn leaflogix-build customers
#' @export
bld_ll_customers <- function(index, org, store) {
  list("customer_summary" = ..bld_ll_customer_summary(index, org, store))
}


#' @describeIn leaflogix-build loyalty
#' @export
bld_ll_loyalty <- function(index, org, store) {
  list("customer_loyalty" = ..bld_ll_customer_loyalty(index, org, store))
}


#' @describeIn leaflogix-build employees
#' @export
bld_ll_employees <- function(index, org, store) {
  list("store_employees" = ..bld_ll_store_employees(index, org, store))
}


#' @describeIn leaflogix-build categories
#' @export
bld_ll_categories <- function(index, org, store) {
  list("category_index" = ..bld_ll_category_index(index, org, store))
}


#' @describeIn leaflogix-build brands
#' @export
bld_ll_brands <- function(index, org, store) {
  list("brand_index" = ..bld_ll_brand_index(index, org, store))
}


#' @describeIn leaflogix-build build consolidated customers
..bld_ll_customer_summary <- function(index, org = NULL, store = NULL) {
  ## add org and store if provided as arguments:
  lmsg <- paste0("Leaflogix|BLD|customer_summary", stringr::str_glue("|{org}.{store}"))

  if (is.null(index)) {
    log_err(paste0(lmsg, "|...No data to consolidate"))
    return(NULL)
  }

  DT <- check_index(index)["customers"][, rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "Customer", lmsg)

  if (is.null(DT)) {
    log_err(paste0(lmsg, "|...Aborting consolidation"))
    return(NULL)
  }

  ## fix list columns, flatten to csv stored as chars
  log_inf(paste0(lmsg, "|...Handling list class columns"))

  DT[, secondaryQualifyingConditions :=
       unlist(lapply(secondaryQualifyingConditions, str_flatten_comma))]
  DT[secondaryQualifyingConditions == "", secondaryQualifyingConditions := NA_character_]

  if (DT[, is.list(discountGroups)]) {
    ## allocating to new column due to memory crash when replacing discountGroup column directly
    DT[sapply(discountGroups, length) == 0, tmp := NA_character_]
    DT[sapply(discountGroups, length) > 0, tmp := sapply(discountGroups, paste0, collapse = ",")]
    DT[, discountGroups := NULL]
    setnames(DT, "tmp", "discountGroups")
  } else {
    stop("Expecting discountGroups column to be of type list")
  }

  ## Not expecting any list class columns at this point
  if (DT[, any(sapply(.SD, is.list))])
    stop("Unexpected list columns still remain in table")

  ## fix dates
  log_inf(paste0(lmsg, "|...Handling date class columns"))

  fmt <- "%m/%d/%Y %H:%M:%S"
  DT[, lastModifiedDateUTC := lubridate::as_datetime(lastModifiedDateUTC, format = fmt)]
  DT[, dateOfBirth := lubridate::as_date(dateOfBirth)]
  DT[, mmjidExpirationDate := lubridate::as_date(mmjidExpirationDate)]
  DT[, creationDate := lubridate::as_datetime(creationDate)]

  DT[lubridate::year(lastModifiedDateUTC) == 1900, lastModifiedDateUTC := lubridate::NA_Date_]
  DT[lubridate::year(dateOfBirth) == 1900, dateOfBirth := lubridate::NA_Date_]
  DT[lubridate::year(mmjidExpirationDate) == 1900, mmjidExpirationDate := lubridate::NA_Date_]
  DT[lubridate::year(creationDate) == 1900, creationDate := lubridate::NA_Date_]

  ## Standardize cols to upper case
  log_inf(paste0(lmsg, "|...Standardizing string columns"))

  cols_to_upper <- c("name", "firstName", "lastName", "middleName",
                     "nameSuffix", "namePrefix", "address1", "address2",
                     "city", "state", "status", "customerType",
                     "gender", "createdByIntegrator", "referralSource",
                     "customIdentifier", "otherReferralSource")
  for (col in cols_to_upper)
    set(DT, NULL, col, str_squish(str_to_upper(DT[, get(col)])))

  cols_to_lower <- c("createdAtLocation", "primaryQualifyingCondition",
                     "secondaryQualifyingConditions", "discountGroups",
                     "notes", "emailAddress")
  for (col in cols_to_lower)
    set(DT, NULL, col, str_squish(str_to_lower(DT[, get(col)])))

  for (j in names(DT[, which(sapply(.SD, is.character, simplify = TRUE))])) {
    DT[get(j) == "N/A", (j) := NA_character_]
    DT[, (j) := str_squish(get(j))]
    DT[str_squish(get(j)) == "", (j) := NA_character_]
  }

  ## make sure ID columns are of char class and fix when 0
  log_inf(paste0(lmsg, "|...Setting id columns to character"))

  for (col in str_subset(names(DT), "Id$"))
    set(DT, NULL, col, as.character(DT[, get(col)]))

  DT[springBigMemberId == "0", springBigMemberId := NA_character_]


  DT[, gender := str_gender(gender)][is.na(gender), gender := "U"]


  ## Merge in org and store UUIDs, reorder columns, set new keys and return
  log_inf(paste0(lmsg, "|...Finalizing data"))
  set_location_uuids(DT) |>
    setcolorder(c("org_uuid", "store_uuid", "org", "store",
                  "customerId", "lastModifiedDateUTC", "creationDate",
                  "externalCustomerId", "mergedIntoCustomerId")) |>
    setkeyv(c("org_uuid", "store_uuid", "lastModifiedDateUTC"))

  log_suc(paste0(lmsg, "|...Consolidated Complete"))
  return(DT[])
}


#' @describeIn leaflogix-build build consolidated customers
..bld_ll_store_employees <- function(index, org = NULL, store = NULL) {

  lmsg <- paste0("Leaflogix|BLD|store_employees", stringr::str_glue("|{org}.{store}"))

  if (is.null(index)) {
    log_err(paste0(lmsg, "|...No data to consolidate"))
    return(NULL)
  }

  DT <- check_index(index)["employees"][, rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "Employee", lmsg)

  if (is.null(DT)) {
    log_err(paste0(lmsg, "|...Aborting consolidation"))
    return(NULL)
  }

  ## dates
  log_inf(paste0(lmsg, "|...Handling date class columns"))

  DT[, mmjExpiration := lubridate::as_datetime(mmjExpiration)]

  DT[lubridate::year(mmjExpiration) == 1900, mmjExpiration := lubridate::NA_Date_]

  ## strings
  log_inf(paste0(lmsg, "|...Standardizing string columns"))

  DT[, defaultLocation := as.character(defaultLocation)]
  DT[, status := stringr::str_to_upper(status)]
  DT[, fullName := stringr::str_to_upper(fullName)]
  DT[, permissionsLocation := stringr::str_to_lower(permissionsLocation)]
  DT[, groups := stringr::str_to_lower(groups)]
  DT[, stateId := stringr::str_to_upper(stateId)]
  DT[, loginId := stringr::str_to_upper(loginId)]

  ## finalize
  log_inf(paste0(lmsg, "|...Finalizing data"))

  set_location_uuids(DT) |>
    setcolorder(c("org_uuid", "store_uuid", "org", "store", "userId", "loginId", "fullName")) |>
    setkeyv(c("org_uuid", "store_uuid", "userId"))

  ## Not expecting any list class columns at this point
  if (DT[, any(sapply(.SD, is.list))])
    stop("Unexpected list columns still remain in table")

  log_suc(paste0(lmsg, "|...Consolidated Complete"))
  return(DT[])
}


#' @describeIn leaflogix-build build consolidated orders table
..bld_ll_order_summary <- function(index, org = NULL, store = NULL) {
  msg_p1 <- "Leaflogix|BLD|order_summary"
  msg_p2 <- stringr::str_glue("|{org}.{store}")
  lmsg <- paste0(msg_p1, msg_p2)

  if (is.null(index)) {
    log_err(paste0(lmsg, "|...No data to consolidate"))
    return(NULL)
  }

  msg <- paste0(msg_p1, " (Orders)", msg_p2)
  s_dt <- check_index(index)["order_summary"][, rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "Transaction", lmsg = msg,
                 ignore = c("items", "discounts", "taxSummary"))

  msg <- paste0(msg_p1, " (Discounts)", msg_p2)
  d_dt <- check_index(index)["order_discount"][, rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "AppliedDiscount", lmsg = msg, include = "transactionId",
                 ignore = "transactionItemId")

  msg <- paste0(msg_p1, " (Taxes)", msg_p2)
  t_dt <- check_index(index)["order_tax"][, rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "TaxSummaryInfo", lmsg = msg, include = "transactionId",
                 ignore = "transactionItemId")

  if (is.null(s_dt) | is.null(d_dt) | is.null(t_dt)) {
    log_err(paste0(lmsg, "|...Aborting consolidation"))
    return(NULL)
  }

  ## Handle list class columns
  log_inf(paste0(lmsg, "|...Handling list class columns"))

  s_dt[, V1 := jsonlite::toJSON(feesAndDonations[[1]], auto_unbox = TRUE),
       keyby = .(transactionId, org, store)]
  s_dt[, feesAndDonations := NULL]
  setnames(s_dt, "V1", "feesAndDonations_json")

  s_dt[, V1 := jsonlite::toJSON(orderIds[[1]], auto_unbox = TRUE),
       keyby = .(transactionId, org, store)]
  s_dt[, orderIds := NULL]
  setnames(s_dt, "V1", "orderIds_json")

  ## Not expecting any list class columns at this point
  if (s_dt[, any(sapply(.SD, is.list))])
    stop("Unexpected list columns still remain in table")


  ## make date columns datetimes
  log_inf(paste0(lmsg, "|...Handling date class columns"))

  date_cols <- c("transactionDate", "voidDate", "checkInDate", "lastModifiedDateUTC",
                 "transactionDateLocalTime", "estDeliveryDateLocal")
  for (col in date_cols)
    set(s_dt, NULL, col, as_datetime(s_dt[, get(col)]))

  ## upper case these columns
  log_inf(paste0(lmsg, "|...Standardizing string columns"))

  s_dt[, transactionType := str_to_upper(transactionType)]
  s_dt[, orderType := str_to_upper(orderType)]
  s_dt[, orderSource := str_to_upper(orderSource)]
  s_dt[, completedByUser := str_to_upper(completedByUser)]
  s_dt[, terminalName := str_to_upper(terminalName)]
  s_dt[, orderMethod := str_to_upper(orderMethod)]
  s_dt[, invoiceName := str_to_upper(invoiceName)]


  ## make id columns character
  log_inf(paste0(lmsg, "|...Setting id columns to character"))

  for (col in str_subset(names(s_dt), "Id$"))
    set(s_dt, NULL, col, as.character(s_dt[, get(col)]))

  for (col in str_subset(names(d_dt), "Id$"))
    set(d_dt, NULL, col, as.character(d_dt[, get(col)]))

  for (col in str_subset(names(t_dt), "Id$"))
    set(t_dt, NULL, col, as.character(t_dt[, get(col)]))


  ## merge in json columns containing discounts and taxes broke out by order
  log_inf(paste0(lmsg, "|...Finalizing data"))

  setkey(s_dt, transactionId, org, store)

  s_dt[
    d_dt[, toJSON(.SD), keyby = .(transactionId, org, store)],
    order_discount_json := V1
  ]

  s_dt[
    t_dt[, toJSON(.SD), keyby = .(transactionId, org, store)],
    order_tax_json := V1
  ]

  c_order <- c("org_uuid", "store_uuid", "org", "store",
               "transactionId", "customerId", "employeeId")

  set_location_uuids(s_dt) |>
    setcolorder(c_order) |>
    setkeyv(c("org_uuid", "store_uuid", "transactionId"))

  log_suc(paste0(lmsg, "|...Consolidated Complete"))
  return(s_dt[])
}


#' @describeIn leaflogix-build build consolidated order lines table
..bld_ll_order_items <- function(index, org = NULL, store = NULL) {
  msg_p1 <- "Leaflogix|BLD|order_items"
  msg_p2 <- stringr::str_glue("|{org}.{store}")
  lmsg <- paste0(msg_p1, msg_p2)

  if (is.null(index)) {
    log_err(paste0(lmsg, "|...No data to consolidate"))
    return(NULL)
  }

  msg <- paste0(msg_p1, " (Items)", msg_p2)
  i_dt <- check_index(index)["order_items"][, rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "TransactionItem", lmsg = msg, ignore = c("discounts", "taxes"))

  msg <- paste0(msg_p1, " (Discounts)", msg_p2)
  d_dt <- check_index(index)["order_items_discount"][, rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "AppliedDiscount", lmsg = msg, include = "transactionId")

  msg <- paste0(msg_p1, " (Taxes)", msg_p2)
  t_dt <- check_index(index)["order_items_tax"][, rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "LineItemTaxInfo", lmsg = msg, include = "transactionId")

  if (is.null(i_dt) | is.null(d_dt) | is.null(t_dt)) {
    log_err(paste0(lmsg, "|...Aborting consolidation"))
    return(NULL)
  }

  ## make date columns datetimes
  log_inf(paste0(lmsg, "|...Handling date class columns"))

  date_cols <- c("returnDate")
  for (col in date_cols)
    set(i_dt, NULL, col, as_datetime(i_dt[, get(col)]))

  ## upper case these columns
  log_inf(paste0(lmsg, "|...Standardizing string columns"))

  i_dt[, vendor := str_to_upper(vendor)]
  i_dt[, returnReason := str_to_upper(returnReason)]
  i_dt[, unitWeightUnit := str_to_upper(unitWeightUnit)]
  i_dt[, flowerEquivalentUnit := str_to_upper(flowerEquivalentUnit)]

  ## make id columns character
  log_inf(paste0(lmsg, "|...Setting id columns to character"))

  for (col in str_subset(names(i_dt), "Id$"))
    set(i_dt, NULL, col, as.character(i_dt[, get(col)]))

  for (col in str_subset(names(d_dt), "Id$"))
    set(d_dt, NULL, col, as.character(d_dt[, get(col)]))

  for (col in str_subset(names(t_dt), "Id$"))
    set(t_dt, NULL, col, as.character(t_dt[, get(col)]))


  ## merge in json columns containing discounts and taxes broke out by order
  log_inf(paste0(lmsg, "|...Finalizing data"))

  setkey(i_dt, transactionId, transactionItemId, org, store)

  i_dt[
    d_dt[, toJSON(.SD), keyby = .(transactionId, transactionItemId, org, store)],
    order_item_discount_json := V1
  ]

  i_dt[
    t_dt[, toJSON(.SD), keyby = .(transactionId, transactionItemId, org, store)],
    order_item_tax_json := V1
  ]

  ## set column order of table and key
  c_order <- c("org_uuid", "store_uuid", "org", "store",
               "transactionId", "transactionItemId", "productId",
               "sourcePackageId")

  set_location_uuids(i_dt) |>
    setcolorder(c_order) |>
    setkeyv(c("org_uuid", "store_uuid", "transactionId", "transactionItemId"))

  ## Not expecting any list class columns at this point
  if (i_dt[, any(sapply(.SD, is.list))])
    stop("Unexpected list columns still remain in table")

  log_suc(paste0(lmsg, "|...Consolidated Complete"))

  return(i_dt[])
}


#' @describeIn leaflogix-build build consolidated loyalty table
..bld_ll_customer_loyalty <- function(index, org = NULL, store = NULL) {
  lmsg <- paste0("Leaflogix|BLD|customer_loyalty", stringr::str_glue("|{org}.{store}"))

  if (is.null(index)) {
    log_err(paste0(lmsg, "|...No data to consolidate"))
    return(NULL)
  }

  DT <- check_index(index)["loyalty"][, rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "LoyaltySnapshot", lmsg)

  if (is.null(DT)) {
    log_err(paste0(lmsg, "|...Aborting consolidation"))
    return(NULL)
  }

  ## id column classes
  log_inf(paste0(lmsg, "|...Setting id columns to character"))

  DT[, customerId := as.character(customerId)]


  ## Finalize and return
  log_inf(paste0(lmsg, "|...Finalizing data"))

  set_location_uuids(DT) |>
    setcolorder(c("org_uuid", "store_uuid", "org", "store", "customerId")) |>
    setkeyv(c("org_uuid", "store_uuid", "customerId"))

  ## Not expecting any list class columns at this point
  if (DT[, any(sapply(.SD, is.list))])
    stop("Unexpected list columns still remain in table")

  log_suc(paste0(lmsg, "|...Consolidated Complete"))
  return(DT[])
}


#' @describeIn leaflogix-build build consolidated products table
..bld_ll_product_summary <- function(index, org = NULL, store = NULL) {
  lmsg <- paste0("Leaflogix|BLD|product_summary", stringr::str_glue("|{org}.{store}"))

  if (is.null(index)) {
    log_err(paste0(lmsg, "|...No data to consolidate"))
    return(NULL)
  }

  DT <- check_index(index)["products"][, rbindlist(data, fill = TRUE)]
  DT[, broadcastedResponses := NULL]
  DT <- check_schema(DT, "leaflogix", "ProductDetail", lmsg)

  if (is.null(DT)) {
    log_err(paste0(lmsg, "|...Aborting consolidation"))
    return(NULL)
  }

  ## handle list columns
  log_inf(paste0(lmsg, "|...Handling list class columns"))

  DT[, tags := NULL]

  ## make empty lists equal to NA first
  empty_indx <- DT[, which(sapply(imageUrls, length) == 0)]
  DT[empty_indx, imageUrls := NA]

  empty_indx <- DT[, which(sapply(pricingTierData, length) == 0)]
  DT[empty_indx, pricingTierData := NA]

  empty_indx <- DT[, which(sapply(taxCategories, length) == 0)]
  DT[empty_indx, taxCategories := NA]

  ## convert to JSON columns
  DT[, V1 := jsonlite::toJSON(imageUrls[[1]], auto_unbox = TRUE),
     keyby = .(productId, sku, org, store)]
  DT[, imageUrls := NULL]
  setnames(DT, "V1", "imageUrls_json")

  DT[, V1 := jsonlite::toJSON(taxCategories[[1]], auto_unbox = TRUE),
     keyby = .(productId, sku, org, store)]
  DT[, taxCategories := NULL]
  setnames(DT, "V1", "taxCategories_json")

  DT[, V1 := jsonlite::toJSON(pricingTierData[[1]], auto_unbox = TRUE),
     keyby = .(productId, sku, org, store)]
  DT[, pricingTierData := NULL]
  setnames(DT, "V1", "pricingTierData_json")

  ## Not expecting any list class columns at this point
  if (DT[, any(sapply(.SD, is.list))])
    stop("Unexpected list columns still remain in table")

  ## Date/times
  log_inf(paste0(lmsg, "|...Handling date class columns"))

  DT[, lastModifiedDateUTC := lubridate::as_datetime(lastModifiedDateUTC)]
  DT[, createdDate := lubridate::as_datetime(createdDate)]

  ## upper case
  log_inf(paste0(lmsg, "|...Standardizing string columns"))

  up_cols <- c("productName",
               "onlineTitle",
               "vendorName",
               "brandName",
               "strainType",
               "strain",
               "internalName",
               "masterCategory",
               "category")
  for (col in up_cols)
    set(DT, NULL, col, stringr::str_to_upper(DT[, get(col)]))

  for (j in names(DT[, which(sapply(.SD, is.character, simplify = TRUE))])) {
    DT[get(j) == "N/A", (j) := NA_character_]
    DT[, (j) := str_squish(get(j))]
    DT[str_squish(get(j)) == "", (j) := NA_character_]
  }

  ## id column classes
  log_inf(paste0(lmsg, "|...Setting id columns to class char"))

  for (col in str_subset(names(DT), "Id$"))
    set(DT, NULL, col, as.character(DT[, get(col)]))

  ## Finalize and return
  log_inf(paste0(lmsg, "|...Finalizing data"))

  set_location_uuids(DT) |>
    setcolorder(c("org_uuid", "store_uuid", "org", "store", "productId", "sku")) |>
    setkeyv(c("org_uuid", "store_uuid", "productId", "sku"))

  log_suc(paste0(lmsg, "|...Consolidated Complete"))
  return(DT[])
}


#' @describeIn leaflogix-build build consolidated inventory tables
..bld_ll_stock_lab_results <- function(index, org = NULL, store = NULL) {
  lmsg <- paste0("Leaflogix|BLD|stock_lab_results", stringr::str_glue("|{org}.{store}"))

  if (is.null(index)) {
    log_err(paste0(lmsg, "|...No data to consolidate"))
    return(NULL)
  }
  incl <- c("packageId", "sampleDate", "testedDate", "productId", "sku",
            "labResultUnitId", "lastModifiedDateUtc")

  DT <- check_index(index)["stock_lab_results", rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "LabResult", lmsg, include = incl)

  if (is.null(DT)) {
    log_err(paste0(lmsg, "|...Aborting consolidation"))
    return(NULL)
  }

  setcolorder(DT, c(str_subset(names(DT), "Id$"), "sku"))

  ## Date columns
  log_inf(paste0(lmsg, "|...Handling date class columns"))

  DT[, sampleDate := lubridate::as_datetime(sampleDate)]
  DT[, lastModifiedDateUtc := lubridate::as_datetime(lastModifiedDateUtc)]
  DT[, testedDate := lubridate::as_datetime(testedDate)]

  ## String col standardization
  log_inf(paste0(lmsg, "|...Standardizing string columns"))

  up_cols <- c("labTest", "labResultUnit")
  for (col in up_cols)
    set(DT, NULL, col, stringr::str_to_upper(DT[, get(col)]))

  ## id columns
  log_inf(paste0(lmsg, "|...Setting id columns to class char"))

  id_cols <- stringr::str_subset(names(DT), "Id$")
  for (col in id_cols)
    set(DT, NULL, col, as.character(DT[, get(col)]))

  ## finalize
  log_inf(paste0(lmsg, "|...Finalizing data"))

  setnames(DT, "value", "labvalue")

  set_location_uuids(DT) |>
    setcolorder(c("org_uuid", "store_uuid", "org", "store", "lastModifiedDateUtc"))

  ## Not expecting any list class columns at this point
  if (DT[, any(sapply(.SD, is.list))])
    stop("Unexpected list columns still remain in table")

  ## return
  log_suc(paste0(lmsg, "|...Consolidated Complete"))
  return(DT)
}


#' @describeIn leaflogix-build build consolidated inventory tables
..bld_ll_stock_by_room <- function(index, org = NULL, store = NULL) {
  lmsg <- paste0("Leaflogix|BLD|stock_by_room", stringr::str_glue("|{org}.{store}"))

  if (is.null(index)) {
    log_err(paste0(lmsg, "|...No data to consolidate"))
    return(NULL)
  }

  incl <- c("packageId", "productId", "sku", "lastModifiedDateUtc")
  DT <- check_index(index)["stock_by_room", rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "InventoryRoomQuantity", lmsg, include = incl)

  if (is.null(DT)) {
    log_err(paste0(lmsg, "|...Aborting consolidation"))
    return(NULL)
  }
  setcolorder(DT, c(str_subset(names(DT), "Id$"), "sku"))

  ## Date columns
  log_inf(paste0(lmsg, "|...Handling date class columns"))
  DT[, lastModifiedDateUtc := lubridate::as_datetime(lastModifiedDateUtc)]

  ## String col standardization
  log_inf(paste0(lmsg, "|...Standardizing string columns"))
  DT[, room := stringr::str_to_upper(room)]

  ## id columns
  log_inf(paste0(lmsg, "|...Setting id columns to class char"))

  DT[, roomId := as.character(roomId)]
  DT[, packageId := as.character(packageId)]
  DT[, productId := as.character(productId)]
  DT[, sku := as.character(sku)]

  ## finalize
  log_inf(paste0(lmsg, "|...Finalizing data"))

  set_location_uuids(DT) |>
    setcolorder(c("org_uuid", "store_uuid", "org", "store", "lastModifiedDateUtc"))

  ## Not expecting any list class columns at this point
  if (DT[, any(sapply(.SD, is.list))])
    stop("Unexpected list columns still remain in table")

  ## return
  log_suc(paste0(lmsg, "|...Consolidated Complete"))
  return(DT)
}


#' @describeIn leaflogix-build build consolidated inventory tables
..bld_ll_stock_summary <- function(index, org = NULL, store = NULL) {
  lmsg <- paste0("Leaflogix|BLD|stock_summary", stringr::str_glue("|{org}.{store}"))

  if (is.null(index)) {
    log_err(paste0(lmsg, "|...No data to consolidate"))
    return(NULL)
  }

  DT <- check_index(index)["stock_summary"][, rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "ReportingInventoryItem", lmsg,
                 ignore =  c("tags", "description", "lineage", "labResults", "roomQuantities"))

  if (is.null(DT)) {
    log_err(paste0(lmsg, "|...Aborting consolidation"))
    return(NULL)
  }

  ## Drop these as they appear in the labresults dataset
  DT[, sampleDate := NULL]
  DT[, testedDate := NULL]
  DT[, labTestStatus := NULL]
  DT[, alternateName := NULL]

  setcolorder(DT, c(
    c("sku", str_subset(names(DT), "Id$")),
    str_subset(names(DT), "Date$"),
    c("quantityAvailable", "productName", "category", "masterCategory", "brandName")
    ))

  ## Date columns
  log_inf(paste0(lmsg, "|...Handling date class columns"))

  date_cols <- c("packagedDate",
                 "manufacturingDate",
                 "lastModifiedDateUtc",
                 "expirationDate")
  for (col in date_cols)
    set(DT, NULL, col, lubridate::as_datetime(DT[, get(col)]))

  ## String col standardization
  log_inf(paste0(lmsg, "|...Standardizing string columns"))

  up_cols <- c("productName",
               "category",
               "quantityUnits",
               "strain",
               "strainType",
               "vendor",
               "brandName",
               "masterCategory")
  for (col in up_cols)
    set(DT, NULL, col, stringr::str_to_upper(DT[, get(col)]))

  ## id columns
  log_inf(paste0(lmsg, "|...Setting id columns to class char"))

  id_cols <- str_subset(names(DT), "Id$")
  for (col in id_cols)
    set(DT, NULL, col, as.character(DT[, get(col)]))

  ## finalize
  log_inf(paste0(lmsg, "|...Finalizing data"))

  set_location_uuids(DT) |>
    setcolorder(c("org_uuid", "store_uuid", "org", "store", "lastModifiedDateUtc"))

  ## Not expecting any list class columns at this point
  if (DT[, any(sapply(.SD, is.list))])
    stop("Unexpected list columns still remain in table")

  ## return
  log_suc(paste0(lmsg, "|...Consolidated Complete"))
  return(DT)
}


#' @describeIn leaflogix-build build consolidated inventory tables
..bld_ll_stock_snapshot <- function(index, org = NULL, store = NULL) {
  lmsg <- paste0("Leaflogix|BLD|stock_snapshot", stringr::str_glue("|{org}.{store}"))

  if (is.null(index)) {
    log_err(paste0(lmsg, "|...No data to consolidate"))
    return(NULL)
  }

  DT <- check_index(index)["stocksnapshot", rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "InventorySnapshot", lmsg)

  if (is.null(DT)) {
    log_err(paste0(lmsg, "|...Aborting consolidation"))
    return(NULL)
  }

  setcolorder(DT, c(str_subset(names(DT), "Id$"), "sku", "room"))

  ## Date columns
  log_inf(paste0(lmsg, "|...Handling date class columns"))
  DT[, snapshotDate := lubridate::as_datetime(snapshotDate)]

  ## String col standardization
  log_inf(paste0(lmsg, "|...Standardizing string columns"))
  up_cols <- c("product", "room", "vendor", "unit")
  for (col in up_cols)
    set(DT, NULL, col, stringr::str_to_upper(DT[, get(col)]))

  ## id columns
  log_inf(paste0(lmsg, "|...Setting id columns to class char"))
  DT[, productId := as.integer(productId)]
  DT[, packageId := as.character(packageId)]
  DT[, batchId := as.character(batchId)]
  DT[, unitId := as.integer(unitId)]
  DT[, sku := as.character(sku)]

  ## finalize
  log_inf(paste0(lmsg, "|...Finalizing data"))
  set_location_uuids(DT) |>
    setcolorder(c("org_uuid", "store_uuid", "org", "store", "snapshotDate"))

  ## Not expecting any list class columns at this point
  if (DT[, any(sapply(.SD, is.list))])
    stop("Unexpected list columns still remain in table")

  ## return
  log_suc(paste0(lmsg, "|...Consolidated Complete"))
  return(DT)
}


#' @describeIn leaflogix-build build consolidated brands table
..bld_ll_brand_index <- function(index, org = NULL, store = NULL) {
  lmsg <- paste0("Leaflogix|BLD|brand_index", stringr::str_glue("|{org}.{store}"))

  if (is.null(index)) {
    log_err(paste0(lmsg, "|...No data to consolidate"))
    return(NULL)
  }

  DT <- check_index(index)["brands", rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "Brand", lmsg)

  if (is.null(DT)) {
    log_err(paste0(lmsg, "|...Aborting consolidation"))
    return(NULL)
  }

  ## String col standardization
  log_inf(paste0(lmsg, "|...Standardizing string columns"))
  set(DT, NULL, "brandName", stringr::str_to_upper(DT[, get("brandName")]))

  ## id columns
  log_inf(paste0(lmsg, "|...Setting id columns to class char"))
  set(DT, NULL, "brandId", as.character(DT[, get("brandId")]))
  setcolorder(DT, "brandId")

  ## Finalize
  log_inf(paste0(lmsg, "|...Finalizing data"))

  set_location_uuids(DT) |>
    setcolorder(c("org_uuid", "store_uuid", "org", "store")) |>
    setkeyv(c("org_uuid", "store_uuid", "brandId"))

  ## Not expecting any list class columns at this point
  if (DT[, any(sapply(.SD, is.list))])
    stop("Unexpected list columns still remain in table")

  ## Return
  log_suc(paste0(lmsg, "|...Consolidated Complete"))
  return(DT[])
}


#' @describeIn leaflogix-build build consolidated brands table
..bld_ll_category_index <- function(index, org = NULL, store = NULL) {
  lmsg <- paste0("Leaflogix|BLD|category_index", stringr::str_glue("|{org}.{store}"))

  DT <- check_index(index)["categories", rbindlist(data, fill = TRUE)] |>
    check_schema("leaflogix", "ProductCategory", lmsg)

  if (is.null(DT)) {
    log_err(paste0(lmsg, "|...Aborting consolidation"))
    return(NULL)
  }

  ## String col standardization
  log_inf(paste0(lmsg, "|...Standardizing string columns"))
  DT[, productCategoryName := stringr::str_to_upper(productCategoryName)]
  DT[, masterCategory := stringr::str_to_upper(masterCategory)]

  ## id columns
  log_inf(paste0(lmsg, "|...Setting id columns to class char"))
  DT[, productCategoryId := as.character(productCategoryId)]

  ## finalize
  log_inf(paste0(lmsg, "|...Finalizing data"))

  DT[DT == ""] <- NA_character_

  set_location_uuids(DT) |>
    setcolorder(c("org_uuid", "store_uuid", "org", "store"))

  ## Not expecting any list class columns at this point
  if (DT[, any(sapply(.SD, is.list))])
    stop("Unexpected list columns still remain in table")

  setnames(DT, "productCategoryId", "categoryId")
  setnames(DT, "productCategoryName", "category")

  log_suc(paste0(lmsg, "|...Consolidated Complete"))
  return(DT[])
}


