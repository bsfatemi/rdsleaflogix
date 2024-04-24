#' Helpers to Get Credentials
#'
#' @import data.table
#' @importFrom DBI dbGetQuery
#' @importFrom rpgconn dbc dbd
#'
#' @examples
#' #active_clients <- dbOrgIndex()
#' #creds_leaflogix <- dbLeaflogixCreds()
#'
#' @name utils-creds
NULL

#' @describeIn utils-creds get list of orgs
#' @export
dbOrgIndex <- function() {
  cn <- rpgconn::dbc("prod2", "hcaconfig")
  on.exit(rpgconn::dbd(cn))
  store_index <- function(cn) {
    qry <- "SELECT org_uuid, store_uuid, short_name as store FROM org_stores"
    setDT(DBI::dbGetQuery(cn, qry), key = "org_uuid")[]
  }
  org_index <- function(cn) {
    qry1 <- "SELECT org_uuid, short_name as org FROM org_info"
    qry2 <- "SELECT * FROM org_pipelines_info"
    res1 <- setkey(data.table(DBI::dbGetQuery(cn, qry1)), org_uuid)
    res2 <- setkey(data.table(DBI::dbGetQuery(cn, qry2)), org_uuid)
    out <- res1[res2[current_client & in_population], .(org_uuid, org)]
    setkey(out, "org_uuid")[]
  }
  store_index(cn)[org_index(cn)]
}

#' @describeIn utils-creds get credentials
#' @export
dbLeaflogixCreds <- function() {
  cn <- rpgconn::dbc("prod2", "hcaconfig")
  on.exit(rpgconn::dbd(cn))
  qry <- "SELECT STORES.org_uuid, STORES.store_uuid, consumerkey, auth
          FROM (SELECT * FROM org_credentials_leaflogix) CREDS
          INNER JOIN (SELECT store_uuid, org_uuid FROM org_stores) STORES
          ON CREDS.store_uuid = STORES.store_uuid"
  setDT(DBI::dbGetQuery(cn, qry), key = c("org_uuid", "store_uuid"))[]
}
