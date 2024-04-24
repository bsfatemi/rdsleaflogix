#' Database Connection
#'
#' @param cfg e.g. prod, devel
#' @param dbin database name
#' @param cn connection object to close
#'
#' @importFrom DBI dbConnect dbDisconnect
#' @importFrom fs path_package path_ext
#' @importFrom stats setNames
#' @importFrom yaml yaml.load_file
#' @importFrom RPostgres Postgres
#'
#' @return connection
#'
#' @name database
NULL

#' @describeIn database connect
#' @export
dbc <- function(cfg, dbin) {
  odbc <- ld_odbc(cfg, dbin)
  do.call(DBI::dbConnect, odbc)
}


#' @describeIn database disconnect
#' @export
dbd <- function(cn) {
  DBI::dbDisconnect(cn)
}


#' @describeIn database TBD
#' @export
ld_odbc <- function(cfg, dbin) {
  ..xdfp <- function(fn) fs::path_package("rdleaflogix", "extdata", fs::path_ext(fn), fn)

  k <- match.arg(cfg, c("devel", "local", "stage", "cypress2", "prod2", "dev2"))
  ll <- yaml::yaml.load_file(..xdfp("odbc.yml"), eval.expr = TRUE)

  ## if its a conn to replica, there's also a password to retrieve, otherwise there isnt
  if (k == "prod2" | k == "dev2") {
    cnArgs <- c(
      ll[["host"]][[k]]["host"],
      ll[["host"]][[k]]["password"],
      ll[["args"]],
      ll[["options"]],
      list(dbname = dbin)
    )
    cnArgs$user <- "cabbage"
  } else {
    cnArgs <- c(
      stats::setNames(ll[["host"]][k], "host"),
      ll[["args"]],
      ll[["options"]],
      list(dbname = dbin)
    )
  }
  return(cnArgs)
}
