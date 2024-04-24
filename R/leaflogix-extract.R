#' Extract Leaflogix JSONs
#'
#' @param json json returned from GET step
#' @param org org short name
#' @param store store short name
#'
#' @import data.table
#' @importFrom stringr str_glue
#' @importFrom DBI dbListFields dbWriteTable
#' @importFrom rdtools log_suc log_err
#' @importFrom jsonlite fromJSON
#'
#' @return Extract functions return a data.table containing extracted
#' jsons except for Transactions, which returns a list of 4 data.tables
#'
#' @name leaflogix-extract
NULL

.try_ext_expr <- {
  substitute(
  tryCatch({
    res <- NULL
    if (is.null(json)) stop("No data to extract")
    res <- ..extract(json)
    if (is.null(res)) stop("Extracted data is NULL")
    rdtools::log_suc(lmsg)
    res
  },
  warning = function(c) {
    rdtools::log_wrn(lmsg, stringr::str_squish(c$message))
    res
  },
  error = function(c) {
    rdtools::log_err(lmsg, stringr::str_squish(c$message))
    NULL
  }))
}


#' @describeIn leaflogix-extract extract population input tables
#' @export
ext_ll_population <- function(json, org, store) {
  if (is.null(json))
    return(NULL)

  jsonll <- jsonlite::fromJSON(json)
  if (!all(sapply(jsonll, length) > 0)) {
    log_err(stringr::str_glue("Leaflogix|EXT|population|{org}.{store}|...Missing data from API"))
    return(NULL)
  }

  transactions <- ext_ll_transactions(jsonll$transactions, org, store) |>
    bld_ll_transactions(org, store)

  products <- ext_ll_products(jsonll$products, org, store) |>
    bld_ll_products(org, store)

  customers <- ext_ll_customers(jsonll$customers, org, store) |>
    bld_ll_customers(org, store)

  index <- rbindlist(list(
    extract_index(transactions),
    extract_index(products),
    extract_index(customers)
  ))
  return(index)
}


#' @describeIn leaflogix-extract extract inventory
#' @export
ext_ll_inventory <- function(json, org, store) {
  if (is.null(json))
    return(NULL)

  ..extract <- function(json) {
    DT <- setDT(jsonlite::fromJSON(json))
    DT[, c("org", "store") := list(org, store)]

    labRes <- DT[, .(org, store, productId, sku, packageId, lastModifiedDateUtc,
                     sampleDate, testedDate, labResults)]
    roomQt <- DT[, .(org, store, productId, sku, packageId, lastModifiedDateUtc,
                     roomQuantities)]

    DT[, labResults := NULL]
    DT[, roomQuantities := NULL]
    DT[, tags := NULL]
    DT[, description := NULL]

    ll <- list(
      "stock_summary" = DT[],
      "stock_lab_results" = labRes[, rbindlist(labResults), .(
        org, store, productId, sku, packageId,  sampleDate, testedDate, lastModifiedDateUtc
      )],
      "stock_by_room" = roomQt[, rbindlist(roomQuantities), keyby = .(
        org, store, productId, sku, packageId, lastModifiedDateUtc
      )]
    )
    extract_index(ll)
  }
  lmsg <- stringr::str_glue("Leaflogix|EXT|/reporting/inventory|{org}.{store}")
  eval(.try_ext_expr)
}


#' @describeIn leaflogix-extract extract brands
#' @export
ext_ll_brands <- function(json, org, store) {
  if (is.null(json))
    return(NULL)

  ..extract <- function(json) {
    DT <- setDT(jsonlite::fromJSON(json))
    DT[, c("org", "store") := list(org, store)]
    extract_index(list("brands" = DT[]))
  }
  lmsg <- stringr::str_glue("Leaflogix|EXT|/brand|{org}.{store}")
  eval(.try_ext_expr)
}


#' @describeIn leaflogix-extract extract categories
#' @export
ext_ll_categories <- function(json, org, store) {
  if (is.null(json))
    return(NULL)

  ..extract <- function(json) {
    DT <- setDT(jsonlite::fromJSON(json))[, c("org", "store") := list(org, store)][]
    extract_index(list("categories" = DT))
  }
  lmsg <- stringr::str_glue("Leaflogix|EXT|/product-category|{org}.{store}")
  eval(.try_ext_expr)
}


#' @describeIn leaflogix-extract extract loyalty
#' @export
ext_ll_loyalty <- function(json, org, store) {
  if (is.null(json))
    return(NULL)

  ..extract <- function(json) {
    DT <- setDT(jsonlite::fromJSON(json))
    DT[, c("org", "store") := list(org, store)][]
    extract_index(list("loyalty" = DT))
  }
  lmsg <- stringr::str_glue("Leaflogix|EXT|/reporting/loyalty-snapshot|{org}.{store}")
  eval(.try_ext_expr)
}


#' @describeIn leaflogix-extract extract employees
#' @export
ext_ll_employees <- function(json, org, store) {
  if (is.null(json))
    return(NULL)

  ..extract <- function(json) {
    DT <- setDT(jsonlite::fromJSON(json))
    DT[, c("org", "store") := list(org, store)][]
    extract_index(list("employees" = DT))
  }
  lmsg <- stringr::str_glue("Leaflogix|EXT|/employees|{org}.{store}")
  eval(.try_ext_expr)
}


#' @describeIn leaflogix-extract extract products
#' @export
ext_ll_products <- function(json, org, store) {
  if (is.null(json))
    return(NULL)

  ..f <- function(json, org, store) {
    ..extract <- function(json) {
      setDT(jsonlite::fromJSON(json))[, c("org", "store") := list(org, store)][]
    }
    lmsg <- stringr::str_glue("Leaflogix|EXT|/products|{org}.{store}")
    eval(.try_ext_expr)
  }
  ll <- list("products" = rbindlist(lapply(jsonlite::fromJSON(json), ..f, org, store)))
  extract_index(ll)
}


#' @describeIn leaflogix-extract extract transactions
#' @export
ext_ll_transactions <- function(json, org, store) {
  if (is.null(json))
    return(NULL)

  ..f <- function(json, org, store) {
    ..extract <- function(json) {
      DT <- setDT(jsonlite::fromJSON(json))

      ## items in transaction
      items <- DT[, rbindlist(items)]
      if (nrow(items) > 0) {
        DT[, items := NULL]
      } else {
        stop("...No order items to extract")
      }


      ## extract discounts in items, add transactionId, and drop raw data column
      items_discounts <- items[, rbindlist(discounts, idcol = "transact_item_row")]

      if (nrow(items_discounts) > 0) {
        items_discounts[, transactionId :=
                          items[(items_discounts$transact_item_row), transactionId]]
        items_discounts[, transact_item_row := NULL]
        items[, discounts := NULL]
      } else {
        stop("...No order items discounts to extract")
      }

      ## taxes in items
      items_taxes <- items[, rbindlist(taxes, idcol = "transact_item_row")]

      if (nrow(items_taxes) > 0) {
        items_taxes[, transactionId := items[(items_taxes$transact_item_row), transactionId]]
        items_taxes[, transact_item_row := NULL]
        items[, taxes := NULL]
      } else {
        stop("...No order items taxes to extract")
      }


      ## discounts applied at order level
      discounts <- DT[, rbindlist(discounts, idcol = "transact_row")]

      if (nrow(discounts) > 0) {
        indx <- discounts$transact_row
        discounts[, transactionId := DT[indx, transactionId]]
        discounts[discountId == 0, discountId := NA]
        discounts[, transactionItemId := NULL]
        discounts[, transact_row := NULL]
        DT[, discounts := NULL]
      } else {
        stop("...No order discounts to extract")
      }


      ## Order tax summary
      taxes <- DT[, rbindlist(taxSummary, idcol = "transact_row")]

      if (nrow(taxes) > 0) {
        indx <- taxes$transact_row
        taxes[, transactionId := DT[indx, transactionId]]
        taxes[, transact_row := NULL]
        DT[, taxSummary := NULL]
      } else {
        stop("...No order taxes to extract")
      }


      ## These columns we don't process, so let's convert to column class json for storing in db
      row2json <- function(x, ...) {
        unlist(jsonlite::toJSON(
          unlist(x),
          auto_unbox = TRUE,
          null = "null",
          ...
        ))
      }
      DT[, feesAndDonations := row2json(feesAndDonations), transactionId]
      DT[, orderIds := row2json(orderIds), transactionId]

      ll <- list(
        order_summary = DT,
        order_tax = taxes,
        order_discount = discounts,
        order_items = items,
        order_items_tax = items_taxes,
        order_items_discount = items_discounts
      )

      ##
      ## append org and store if data exists
      ##
      for (d in ll)
        d[, c("org", "store") := list(org, store)]
      return(ll)
    }
    lmsg <- stringr::str_glue("Leaflogix|EXT|/reporting/transactions|{org}.{store}")
    eval(.try_ext_expr)
  }
  json_ll <- jsonlite::fromJSON(json)
  ll <- lapply(json_ll, ..f, org, store)

  for (l in ll)
    if (is.null(l)) return(NULL)

  ll <- list(
    order_summary = rbindlist(lapply(ll, "[[", "order_summary")),
    order_tax = rbindlist(lapply(ll, "[[", "order_tax")),
    order_discount = rbindlist(lapply(ll, "[[", "order_discount")),
    order_items = rbindlist(lapply(ll, "[[", "order_items")),
    order_items_tax = rbindlist(lapply(ll, "[[", "order_items_tax")),
    order_items_discount = rbindlist(lapply(ll, "[[", "order_items_discount"))
  )
  extract_index(ll)
}


#' @describeIn leaflogix-extract extract customers
#' @export
ext_ll_customers <- function(json, org, store) {
  if (is.null(json))
    return(NULL)

  ..f <- function(json, org, store) {
    ..extract <- function(json) {
      DT <- setDT(jsonlite::fromJSON(json))
      DT[, c("org", "store") := list(org, store)][]
    }
    lmsg <- stringr::str_glue("Leaflogix|EXT|/reporting/customers|{org}.{store}")
    eval(.try_ext_expr)
  }
  ll <- list("customers" = rbindlist(lapply(jsonlite::fromJSON(json), ..f, org, store)))
  extract_index(ll)
}


#' @describeIn leaflogix-extract extract stock_snapshot
#' @export
ext_ll_stocksnapshot <- function(json, org, store) {
  if (is.null(json))
    return(NULL)

  ..f <- function(json, org, store) {
    ..extract <- function(json) {
      setDT(jsonlite::fromJSON(json))[, c("org", "store") := list(org, store)][]
    }
    lmsg <- stringr::str_glue("Leaflogix|EXT|/inventory/snapshot|{org}.{store}")
    eval(.try_ext_expr)
  }
  ll <- list("stocksnapshot" = rbindlist(lapply(jsonlite::fromJSON(json), ..f, org, store)))
  extract_index(ll)
}

