#' Utilities to execute pipeline
#'
#' @param org org short name
#' @param store store short name
#' @param endpt api endpoint
#' @param pl The name of the pos pipeline (e.g. 'leaflogix')
#' @param x extracted data as data.table or list of data.tables
#' @param tab The internal name assigned to an endpoint ('transactions')
#' @param argll internal - advanced use only
#' @param test_run internal - advanced use only
#' @param org_uuids internal - advanced use only
#' @param res internal - advanced use only
#' @param DT internal - advanced use only
#'
#' @import data.table
#' @importFrom rdtools log_suc log_wrn log_err log_inf
#'
#' @name utils-run
NULL

#' @describeIn utils-run get arguments for leaflogix run
#' @export
get_leaflogix_args <- function(org, store, endpt, test_run) {
  argsDT <- rbindlist(get_pipeline_args("leaflogix"))
  if (!is.null(org)) {
    argsDT <- setkey(argsDT, org)[org]
    if (!is.null(store))
      argsDT <- setkey(argsDT, store)[store]
  }

  if (!is.null(endpt))
    argsDT <- setkey(argsDT, tab)[endpt]

  argsll <- setNames(split_by_row(argsDT), argsDT[, paste0(org, ".", store)])

  if (test_run)
    argsll <- sample(argsll, min(c(10, length(argsll))))
  stopifnot(.check_pl_arg(argsll))
  return(argsll)
}

#' @describeIn utils-run lookup configured names of the endpoints for POS and required input arg for \code{extract_index}
#' @export
get_ep_names <- function(pl = "leaflogix") {
  .pos_config[[pl]]$endpoints
}

#' @describeIn utils-run lookup configured names of the DB tables
#' @export
get_db_names <- function(pl = "leaflogix") {
  .pos_config[[pl]]$db$consolidated
}

#' @describeIn utils-run create index required by build functions given an extracted data.table and endpt
 extract_index <- function(x, org = NULL, store = NULL) {
  if (is.null(x)) {
    DT <- data.table(
      org = org,
      store = store,
      name = ,
      data = list(NULL),
      tot_rows = 0
    )
  } else {
    DT <- rbindlist(lapply(names(x), function(i) {
      dt <- x[[i]]

      if (is.null(dt) || nrow(dt) == 0) {
        if (!(is.null(org) | is.null(store))) {
          OUT <- data.table(
            org = org,
            store = store,
            name = i,
            data = list(NULL),
            tot_rows = 0
          )
        } else {
          OUT <- NULL
        }
      } else {
        OUT <- dt[, list(
          org = org[1],
          store = store[1],
          name = i,
          data = list(.SD),
          tot_rows = .N
        )]
      }
      return(OUT)
    }))
  }
  return(DT)
}


#' @describeIn utils-run internal
.check_pl_arg <- function(argll) {
  log_inf("...Checking arguments to pipeline run")
  tryCatch({
    stopifnot(is.list(argll) & length(argll) >= 1)
    stopifnot(all(sapply(argll, is.data.table)))
    stopifnot(all(sapply(argll, length) >= 5))
    setcolorder(rbindlist(argll), c("org", "store", "auth", "consumerkey", "tab"))
    TRUE
  }, error = function(c) {
    log_err("...Pipeline args empty or invalid")
    FALSE
  }, warning = function(c) {
    log_err("...Pipeline args empty or invalid")
    FALSE
  })
}


#' @describeIn utils-run validate inputs to pipeline run 'argll
check_inputs <- function(argll = NULL, test_run = FALSE, org_uuids = NULL) {

  ## if none provided in call, get all args
  if (is.null(argll)) {
    log_inf("...Getting pipeline inputs")
    argll <- get_pipeline_args("leaflogix", org_uuids = org_uuids)
  }

  if (.check_pl_arg(argll)) {
    log_suc("...Validated inputs for pipeline")
  } else {
    log_err("...Aborting run")
    return(NULL)
  }

  ## if test run, downsample args
  if (test_run) {
    log_inf("...Test run flag is true. Downsampling args")
    argll <- argll[1:10]
  }

  return(argll)
}

#' @describeIn utils-run check the results
check_result <- function(res = NULL) {
  check <- c(is.list(res),
             "data" %in% names(res),
             "logs" %in% names(res),
             is.data.table(res$data),
             is.data.table(res$logs),
             has_rows(res$data),
             has_rows(res$logs))
  if (!all(check))
    stop("Invalid results detected")
  invisible(TRUE)
}


#' @describeIn utils-run lookup data flow
lookupIO <- function(pl, tab) {
  if (pl == "leaflogix") {
    OUT <- switch(
      tab,
      transactions = list(
        endpoint = "/reporting/transactions",
        order_summary = c("order_summary", "order_tax", "order_discount"),
        order_items = c("order_items", "order_items_tax", "order_items_discount")
      ),
      customers = list(
        endpoint = "/reporting/customers",
        customer_summary = "customers"
      ),
      employees = list(
        endpoint = "/employees",
        store_employees = "employees"
      ),
      inventory = list(
        endpoint = "/reporting/inventory",
        stock_summary = "stock_summary",
        stock_by_room = "stock_by_room",
        stock_lab_results = "stock_lab_results"
      ),
      stocksnapshot = list(
        endpoint = "/inventory/snapshot",
        stock_snapshot = "stocksnapshot"
        ),
      loyalty = list(
        endpoint = "/reporting/loyalty-snapshot",
        customer_loyalty = "loyalty"
        ),
      brands = list(
        endpoint = "/brand",
        brand_index = "brands"
        ),
      categories = list(
        endpoint = "/product-category",
        category_index = "categories"
        ),
      products = list(
        endpoint = "/products",
        product_summary = "products"
        )
    )
  } else {
    stop("POS Not Configured")
  }
  return(OUT)
}

#' @describeIn utils-run lookup data flow
lookupIO_All <- function(pl) {
  list(
    transactions = lookupIO("leaflogix", "transactions"),
    customers = lookupIO("leaflogix", "customers"),
    employees = lookupIO("leaflogix", "employees"),
    inventory = lookupIO("leaflogix", "inventory"),
    stocksnapshot = lookupIO("leaflogix", "stocksnapshot"),
    loyalty = lookupIO("leaflogix", "loyalty"),
    brands = lookupIO("leaflogix", "brands"),
    categories = lookupIO("leaflogix", "categories"),
    products = lookupIO("leaflogix", "products")
  )
}


#' @describeIn utils-run returns FALSE if NULL or has no rows
has_rows <- function(DT) {
  if (is.null(DT))
    return(FALSE)
  stopifnot(is.data.table(DT))
  return(DT[, .N > 0])
}


#' @describeIn utils-run create index required by build functions given an extracted data.table and endpt
#' @export
lookup_pl_funs <- function(tab) {
  list(
    get = sapply(tab, function(x) get(paste0("get_ll_", x)), simplify = FALSE),
    ext = sapply(tab, function(x) get(paste0("ext_ll_", x)), simplify = FALSE),
    bld = sapply(tab, function(x) get(paste0("bld_ll_", x)), simplify = FALSE)
  )
}


