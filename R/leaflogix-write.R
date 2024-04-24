#' Write to Database
#'
#' @param ll List of tables to write to database consolidated and schema leaflogix
#' @param org org name for write
#' @param store store name for write
#'
#' @import data.table
#' @importFrom DBI Id dbWithTransaction dbSendQuery dbClearResult dbGetQuery dbAppendTable
#' @importFrom lubridate today
#' @importFrom stringr str_glue str_to_lower
#' @importFrom rdtools log_suc log_wrn log_inf log_err
#'
#' @name leaflogix-write
NULL

#' @describeIn leaflogix-write  write population
#' @export
wrt_ll_population <- function(ll, org, store) {
  ..wrt <- function(DT, org, store) {
    lmsg <- stringr::str_glue("Leaflogix|WRT|Population|{org}.{store}")

    cn <- dbc("dev2", "integrated")
    on.exit(dbd(cn))

    log_inf(paste0(lmsg, "|...Checking existing orders in DB"))

    ## query for existing order_id and order_line_ids
    qry <- stringr::str_glue(
      "SELECT order_id, order_line_id
     FROM population
     WHERE org = '{org}'
      AND store = '{store}'"
    )

    filt_out <- setDT(dbGetQuery(cn, qry))[]
    setkey(filt_out, order_id, order_line_id)
    setkey(DT, order_id, order_line_id)

    OUT <- DT[!filt_out]

    ## log count of rows dropped
    if (nrow(OUT) < nrow(DT)) {
      msg <- stringr::str_glue("|...Dropped {nrow(DT) - nrow(OUT)} rows found in DB")
      log_wrn(paste0(lmsg, msg))
    }
    n_new <- DBI::dbAppendTable(cn, "population", OUT)
    log_suc(stringr::str_glue("...Appended {n_new} new rows to population"))

    invisible(OUT)
  }
  ..wrt(ll[["population"]], org, store)
}


#' @describeIn leaflogix-write transactions
#' @export
wrt_ll_transactions <- function(ll, org, store) {
  ..wrt_o <- function(DT, org, store) {
    stop("Not implemented yet")
  }
  ..wrt_i <- function(DT, org, store) {
    stop("Not implemented yet")
  }
  ..wrt_o(ll[["order_summary"]], org, store)
  ..wrt_i(ll[["order_items"]], org, store)
}


#' @describeIn leaflogix-write stock snapshot
#' @export
wrt_ll_stocksnapshot <- function(ll, org, store) {
  lmsg <- stringr::str_glue("Leaflogix|WRT|stock_snapshot|{org}.{store}")

  ..wrt <- function(DT, org, store) {
    db_current_snap <- function(cn, org, store) {
      tab <- DBI::Id(schema = "leaflogix", table = "stock_snapshot")
      qry <- stringr::str_glue(
        "SELECT productid, sku, packageid, max(snapshotdate) AS latest_snapshotdate
    FROM leaflogix.stock_snapshot
    WHERE org = '{org}'
      AND store = '{store}'
  GROUP BY productid, sku, packageid"
      )
      setDT(DBI::dbGetQuery(cn, qry))[]
    }

    setnames(DT, stringr::str_to_lower(names(DT)))

    tab <- DBI::Id(schema = "leaflogix", table = "stock_snapshot")

    cn <- dbc("dev2", "consolidated")
    on.exit(dbd(cn))

    curr_snap <- db_current_snap(cn, org, store)

    if (nrow(curr_snap) > 0) {
      keyCols <- c("packageid", "productid", "sku")
      setkeyv(DT, keyCols)
      setkeyv(curr_snap, keyCols)
      OUT <- curr_snap[unique(DT)][snapshotdate > latest_snapshotdate, !"latest_snapshotdate"]
    } else {
      OUT <- unique(DT)
    }

    OUT[is.na(batchid), c("batchid", "batchname") := as.integer(snapshotdate) + .I]

    n <- tryCatch({
      DBI::dbAppendTable(cn, tab, OUT)
    }, error = function(c) {
      log_err(paste0(lmsg, "|...Failed writing to DB"), c$message)
      NULL
    })

    if (is.null(n))
      return(NULL)

    if (n == 0) {
      log_wrn(paste0(lmsg, "|...No new rows to append to DB"))
    } else {
      log_suc(paste0(lmsg, stringr::str_glue("|...Appended {n} new rows to DB")))
    }
    return(invisible(OUT))
  }
  ..wrt(ll[["stock_snapshot"]], org, store)
}


#' @describeIn leaflogix-write inventory
#' @export
wrt_ll_inventory <- function(ll, org, store) {
  ..wrt_s <- function(DT, org, store) {
    stop("Not implemented yet")
  }
  ..wrt_r <- function(DT, org, store) {
    stop("Not implemented yet")
  }
  ..wrt_l <- function(DT, org, store) {
    stop("Not implemented yet")
  }
  ..wrt_s(ll[["stock_summary"]], org, store)
  ..wrt_r(ll[["stock_by_room"]], org, store)
  ..wrt_l(ll[["stock_lab_results"]], org, store)
}


#' @describeIn leaflogix-write products
#' @export
wrt_ll_products <- function(ll, org, store) {
  ..wrt <- function(DT, org, store) {
    stop("Not implemented yet")
  }
  ..wrt(ll[["product_summary"]], org, store)
}


#' @describeIn leaflogix-write customers
#' @export
wrt_ll_customers <- function(ll, org, store) {
  ..wrt <- function(DT, org, store, clear_rows = FALSE) {
    if (is.null(DT) || nrow(DT) == 0) {
      log_err("No data to write to db")
      return(NULL)
    }

    setnames(DT, str_to_lower(names(DT)))
    DT[, customerid := as.integer(customerid)]
    id <- Id(schema = "leaflogix", table = "customer_summary")

    cn <- dbc("dev2", "consolidated")
    on.exit(dbd(cn))

    DBI::dbWithTransaction(cn, {
      if (clear_rows) {
        res <- dbSendQuery(cn, "TRUNCATE TABLE leaflogix.customer_summary;")
        dbClearResult(res)
      }

      ## add run date and append
      org_stores <- DT[, .N, .(org_uuid, store_uuid)][, !"N"]

      out <- apply(org_stores, 1, function(row) {
        oid <- row["org_uuid"]
        sid <- row["store_uuid"]
        qry <- stringr::str_glue("SELECT customerid, lastmodifieddateutc
                           FROM leaflogix.customer_summary
                           WHERE org_uuid = '{oid}' AND store_uuid = '{sid}'")
        index <- setDT(dbGetQuery(cn, qry))[, max(lastmodifieddateutc), keyby = customerid]
        OUT <- setkey(
          DT[org_uuid == oid & store_uuid == sid],
          customerid
        )[index][lastmodifieddateutc > V1, !"V1"]

        OUT[, run_date := lubridate::today()]

        msg <- "customer_summary|appending {nrow(OUT)} rows|{OUT[, org[1]]}.{OUT[, store[1]]}"

        log_inf(stringr::str_glue(msg))
        dbAppendTable(cn, id, OUT)
      })
      out
    })
  }
  ..wrt(ll[["customer_summary"]], org, store)
}


#' @describeIn leaflogix-write loyalty
#' @export
wrt_ll_loyalty <- function(ll, org, store) {
  ..wrt <- function(DT, org, store) {
    stop("Not implemented yet")
  }
  ..wrt(ll[["customer_loyalty"]], org, store)
}


#' @describeIn leaflogix-write employees
#' @export
wrt_ll_employees <- function(ll, org, store) {
  ..wrt <- function(DT, org, store) {
    stop("Not implemented yet")
  }
  ..wrt(ll[["store_employees"]], org, store)
}


#' @describeIn leaflogix-write categories
#' @export
wrt_ll_categories <- function(ll, org, store) {
  ..wrt <- function(DT, org, store) {
    if (is.null(DT) || nrow(DT) == 0) {
      log_err("No data to write to db")
      return(NULL)
    }

    cn <- dbc("dev2", "consolidated")
    on.exit(dbd(cn))

    nam <- DBI::Id(schema = "leaflogix", table = "category_index")

    setnames(DT, stringr::str_to_lower(names(DT)))
    DT[, "categoryid" := as.integer(categoryid)]
    DT[, "date_added" := lubridate::today()]

    qry <- stringr::str_glue(
      "SELECT *
      FROM leaflogix.category_index
      WHERE org = '{org}'
        AND store = '{store}'"
    )
    OUT <- setkey(DT, categoryid)[!setDT(DBI::dbGetQuery(cn, qry), key = "categoryid")]

    if (nrow(OUT) > 0) {
      n <- DBI::dbAppendTable(cn, nam, OUT)
      log_suc(stringr::str_glue("Added {n} new categories to the database"))
      return(n)
    } else {
      log_wrn("No new categories to add to the database")
      return(NULL)
    }
  }
  ..wrt(ll[["category_index"]], org, store)
}


#' @describeIn leaflogix-write brands
#' @export
wrt_ll_brands <- function(ll, org, store) {
  ..wrt <- function(DT, org, store) {
    if (is.null(DT) || nrow(DT) == 0) {
      log_err("No data to write to db")
      return(NULL)
    }

    cn <- dbc("dev2", "consolidated")
    on.exit(dbd(cn))

    nam <- DBI::Id(schema = "leaflogix", table = "brand_index")

    setnames(DT, stringr::str_to_lower(names(DT)))
    DT[, "brandid" := as.integer(brandid)]
    DT[, "date_added" := lubridate::today()]

    qry <- stringr::str_glue(
      "SELECT *
      FROM leaflogix.brand_index
      WHERE org = '{org}'
        AND store = '{store}'"
    )
    OUT <- setkey(DT, brandid)[!setDT(DBI::dbGetQuery(cn, qry), key = "brandid")]

    if (nrow(OUT) > 0) {
      n <- DBI::dbAppendTable(cn, nam, OUT)
      log_suc(stringr::str_glue("Added {n} new brands to the database"))
      return(n)
    } else {
      log_wrn("No new brands to add to the database")
      return(NULL)
    }
  }
  ..wrt(ll[["brand_index"]], org, store)
}

