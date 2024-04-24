#' Get API Keys for GET Pipelines
#'
#' @param pl The name of the pos pipeline (e.g. 'leaflogix')
#' @param org_uuids optional. If provided, pipeline arguments will be filtered on these orgs only
#' @param url internal - advanced use only
#' @param ... internal - advanced use only
#' @param endpt internal - advanced use only
#' @param query internal - advanced use only
#' @param org org short name
#' @param store store short name
#'
#' @import data.table
#' @importFrom httr build_url GET stop_for_status parse_url content with_verbose
#' @importFrom jsonlite validate
#' @importFrom rdtools log_wrn log_err
#'
#' @name utils-get
NULL

#' @describeIn utils-get lookup of API keys configured for every location given pipeline name
#' @export
read_org_index <- function() {
  dbOrgIndex()
}


#' @describeIn utils-get lookup of API keys configured for every location given pipeline name
#' @export
read_brand_patterns <- function() {
  brand_patterns[]
}


#' @describeIn utils-get lookup index of hca orgs and stores
#' @export
read_leaflogix_creds <- function() {
  dbLeaflogixCreds()
}


#' @describeIn utils-get Get an index table containing every org store and credentials
#' @export
get_leaflogix_index <- function(org = NULL, store = NULL) {
  x <- read_leaflogix_creds()[read_org_index(), on = c("org_uuid", "store_uuid"), nomatch = NULL]
  if (!is.null(org)) {
    x <- setkey(x, org)[org]
    if (!is.null(store))
      x <- setkey(x, store)[store]
  }
  return(x)
}

#' @describeIn utils-get Get inputs needed to kick off any POS get/extract pipelines
#' @export
get_pipeline_args <- function(pl = "leaflogix", org_uuids = NULL) {

  ## endpoints defined for this pos
  endpts <- .pos_config[[pl]]$endpoints

  # orgs w/api access for pos
  if (pl == "leaflogix") {
    CR <- setkey(read_leaflogix_creds(), org_uuid, store_uuid)
  } else {
    stop("Only configured for leaflogix")
  }

  # filter if supplied and reset key on table
  if (!is.null(org_uuids)) {
    CR <- setkey(CR[.(org_uuids), nomatch = NULL], org_uuid, store_uuid)
    if (nrow(CR) == 0) {
      log_err("...orgs given have no api access")
      stop("Aborting run")
    }
    if (CR[, .N, org_uuid][, .N < length(org_uuids)]) {
      log_wrn("...Dropped orgs w/no api access")
    }
  }

  ## get org index to merge in short names
  DT <- setkey(read_org_index(), org_uuid, store_uuid)

  ## make a subset of the joined tables for every endpoint, while adding column and setting key
  OUT <- CR[DT, nomatch = NULL][, rbindlist(lapply(endpts, function(i) copy(.SD)[, tab := i]))]

  if (OUT[, .N, org_uuid][, .N < CR[, .N, org_uuid][, .N]])
    log_wrn("...Dropped orgs missing from orgIndexFull")

  if (OUT[, .N == 0]) {
    log_err("...No valid orgs to run pipeline")
    stop("Aborting run")
  }

  # ## TODO - enable these to flow through pipeline
  OUT[, org_uuid := NULL]
  OUT[, store_uuid := NULL]

  ## randomize order for load distribution of high volume endpoints, split and return
  OUT[sample(1:.N)] |>
    split_by_row()
}


#' @describeIn utils-get internal wrapper around httr::GET
..g <- function(url, ...) {
  resp <- httr::GET(url, ...) |>
    httr::stop_for_status(paste0("GET...", httr::parse_url(url)[["hostname"]]))

  ## check status before parsing
  parsed <- httr::content(resp, "text", encoding = "UTF-8")

  ## check parsed for empty or invalid json
  if (parsed == "[]")
    stop("Empty json received from API", call. = FALSE)
  if ( !jsonlite::validate(parsed) )
    stop("Invalid json in API response")

  return(parsed)
}


#' @describeIn utils-get internal tool to build the request URL
..api <- function(pl, endpt, query = NULL) {
  purl <- switch(
    pl,
    leaflogix = httr::parse_url("https://publicapi.leaflogix.net"),
    blaze = stop("Not configured yet"),
    webjoint = stop("Not configured yet"),
    stop("POS not found")
  )
  purl$path <- endpt
  purl$query <- query
  httr::build_url(purl)
}


