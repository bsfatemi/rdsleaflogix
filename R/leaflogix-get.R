#' Leaflogix Get Endpoint Functions
#'
#' @param org Org short name
#' @param store store short name
#' @param auth location auth key
#' @param consumerkey location consumer key
#' @param ... internal - advanced use only
#'
#' @importFrom rdtools log_suc log_err
#' @importFrom stringr str_glue
#' @importFrom lubridate today days seconds floor_date ceiling_date
#' @importFrom httr parse_url add_headers accept_json build_url GET stop_for_status content
#' @importFrom jsonlite validate
#' @importFrom rlang has_name
#'
#' @name leaflogix-get
NULL

.try_get_expr <- {
  substitute(
    tryCatch({
      res <- ..g(
        api_url,
        httr::accept_json(),
        httr::add_headers(
          ConsumerKey = consumerkey,
          Authorization = paste("Basic", auth)
        ))
      rdtools::log_suc(lmsg)
      res
    }, warning = function(c) {
      rdtools::log_wrn(lmsg, stringr::str_squish(c$message))
      res
    }, error = function(c) {
      rdtools::log_err(lmsg, stringr::str_squish(c$message))
      NULL
    })
  )
}


#' @describeIn leaflogix-get Get current inventory
#' @export
get_ll_population <- function(org, store, auth, consumerkey, ...) {
  days <- list(...)$n
  days[is.null(days)] <- 1
  ll <- list(
    "transactions" = get_ll_transactions(org, store, auth, consumerkey, n = days),
    "products" = get_ll_products(org, store, auth, consumerkey, n = days),
    "customers" = get_ll_customers(org, store, auth, consumerkey, n = days)
  )
  json <- jsonlite::toJSON(ll, auto_unbox = TRUE)
  return(json)
}


#' @describeIn leaflogix-get Get current inventory
#' @export
get_ll_inventory <- function(org, store, auth, consumerkey) {
  q <- list(
    includeLabResults = "TRUE",
    includeRoomQuantities = "TRUE",
    includeAllocated = "TRUE",
    includeLineage = "FALSE"
  )
  api_url <- ..api("leaflogix", "/reporting/inventory", q)
  lmsg <- stringr::str_glue("Leaflogix|GET|/reporting/inventory|{org}.{store}")
  eval(.try_get_expr)
}


#' @describeIn leaflogix-get Get inventory snapshot
#' @export
get_ll_stocksnapshot <- function(org, store, auth, consumerkey, ...) {
  ..get_hist <- function(org, store, auth, consumerkey, n = 90) {
    ll <- lapply(lubridate::today() - lubridate::days(1:n), function(i) {
      api_url <- ..api("leaflogix", "/inventory/snapshot", list(fromDate = i))
      lmsg <- stringr::str_glue("Leaflogix|GET|/inventory/snapshot...{i}|{org}.{store}")
      eval(.try_get_expr)
    })
    tmp <- unlist(ll, recursive = FALSE)
    if (is.null(tmp))
      return(tmp)
    jsonlite::toJSON(tmp, auto_unbox = TRUE)
  }

  if (rlang::has_name(list(...), name = "n")) {
    ..get_hist(org, store, auth, consumerkey, n = list(...)$n)
  } else {
    ..get_hist(org, store, auth, consumerkey, n = 1)
  }
}


#' @describeIn leaflogix-get Get master brands list
#' @export
get_ll_brands <- function(org, store, auth, consumerkey) {
  api_url <- ..api("leaflogix", "/brand")
  lmsg <- stringr::str_glue("Leaflogix|GET|/brand|{org}.{store}")
  eval(.try_get_expr)
}


#' @describeIn leaflogix-get Get master categories list
#' @export
get_ll_categories <- function(org, store, auth, consumerkey) {
  api_url <- ..api("leaflogix", "/product-category")
  lmsg <- stringr::str_glue("Leaflogix|GET|/product-category|{org}.{store}")
  eval(.try_get_expr)
}


#' @describeIn leaflogix-get Get transactions
#' @export
get_ll_transactions <- function(org, store, auth, consumerkey, ...) {
  ..get_hist <- function(org, store, auth, consumerkey, n = 90) {
    ll <- lapply(lubridate::today() - lubridate::days(1:n), function(i) {
      t0 <- lubridate::floor_date(i, unit = "day") + lubridate::seconds(1)
      t1 <- lubridate::ceiling_date(i, unit = "day") - lubridate::seconds(1)
      q <- list(
        fromLastModifiedDateUTC = t0,
        toLastModifiedDateUTC = t1,
        includeDetail = "TRUE",
        includeTaxes = "TRUE",
        includeOrderIds = "FALSE"
      )
      api_url <- ..api("leaflogix", "/reporting/transactions", q)
      lmsg <- stringr::str_glue("Leaflogix|GET|/reporting/transactions...{i}|{org}.{store}")
      eval(.try_get_expr)
    })
    tmp <- unlist(ll, recursive = FALSE)
    if (is.null(tmp))
      return(tmp)
    jsonlite::toJSON(tmp, auto_unbox = TRUE)
  }

  if (rlang::has_name(list(...), name = "n")) {
    ..get_hist(org, store, auth, consumerkey, n = list(...)$n)
  } else {
    ..get_hist(org, store, auth, consumerkey, n = 1)
  }
}


#' @describeIn leaflogix-get Get products
#' @export
get_ll_products <- function(org, store, auth, consumerkey, ...) {
  ..get_hist <- function(org, store, auth, consumerkey, n = 1) {
    ll <- lapply(lubridate::today() - lubridate::days(1:n), function(i) {
      api_url <- ..api("leaflogix", "/products", list(fromLastModifiedDateUTC = i))
      lmsg <- stringr::str_glue("Leaflogix|GET|/products...{i}|{org}.{store}")
      eval(.try_get_expr)
    })
    tmp <- unlist(ll, recursive = FALSE)
    if (is.null(tmp))
      return(tmp)
    jsonlite::toJSON(tmp, auto_unbox = TRUE)
  }

  if (rlang::has_name(list(...), name = "n")) {
    ..get_hist(org, store, auth, consumerkey, n = list(...)$n)
  } else {
    ..get_hist(org, store, auth, consumerkey, n = 1)
  }
}


#' @describeIn leaflogix-get Get loyalty
#' @export
get_ll_loyalty <- function(org, store, auth, consumerkey) {
  api_url <- ..api("leaflogix", "/reporting/loyalty-snapshot")
  lmsg <- stringr::str_glue("Leaflogix|GET|/reporting/loyalty-snapshot|{org}.{store}")
  eval(.try_get_expr)
}


#' @describeIn leaflogix-get Get employees
#' @export
get_ll_employees <- function(org, store, auth, consumerkey) {
  api_url <- ..api("leaflogix", "/employees")
  lmsg <- stringr::str_glue("Leaflogix|GET|/employees|{org}.{store}")
  eval(.try_get_expr)
}


#' @describeIn leaflogix-get Get customers
#' @export
get_ll_customers <- function(org, store, auth, consumerkey, ...) {
  ..get_hist <- function(org, store, auth, consumerkey, n = 90) {
    ll <- lapply(lubridate::today() - lubridate::days(1:n), function(i) {
      t0 <- lubridate::floor_date(i, unit = "day") + lubridate::seconds(1)
      t1 <- lubridate::ceiling_date(i, unit = "day") - lubridate::seconds(1)
      q <- list(
        fromLastModifiedDateUTC = t0,
        toLastModifiedDateUTC = t1,
        includeAnonymous = "TRUE"
      )
      api_url <- ..api("leaflogix", "/reporting/customers", q)
      lmsg <- stringr::str_glue("Leaflogix|GET|/reporting/customers...{i}|{org}.{store}")
      eval(.try_get_expr)
    })
    tmp <- unlist(ll, recursive = FALSE)
    if (is.null(tmp))
      return(tmp)
    jsonlite::toJSON(tmp, auto_unbox = TRUE)
  }

  if (rlang::has_name(list(...), name = "n")) {
    ..get_hist(org, store, auth, consumerkey, n = list(...)$n)
  } else {
    ..get_hist(org, store, auth, consumerkey, n = 1)
  }
}



