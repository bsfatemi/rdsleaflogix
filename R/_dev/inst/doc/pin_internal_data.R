## ----setup, include=FALSE-----------------------------------------------------
library(data.table)
library(DBI)
library(lubridate)
library(pins)
library(rdleaflogix)
library(stringr)

## -----------------------------------------------------------------------------
apiKey <- Sys.getenv("CONNECT_API_KEY")
apiUrl <- Sys.getenv("CONNECT_SERVER")
board  <- board_connect(server = apiUrl, key = apiKey)

## -----------------------------------------------------------------------------
dbOrgIndex <- function() {
  store_index <- function() {
    cn <- dbc(Sys.getenv("HCA_ENV", "prod2"), "hcaconfig")
    on.exit(dbd(cn))
    qry <- "SELECT org_uuid, store_uuid, short_name as store FROM org_stores"
    setDT(DBI::dbGetQuery(cn, qry), key = "org_uuid")[]
  }
  org_index <- function() {
    cn <- dbc(Sys.getenv("HCA_ENV", "prod2"), "hcaconfig")
    on.exit(dbd(cn))
    query_1 <- "SELECT org_uuid, short_name as org FROM org_info"
    query_2 <- "SELECT * FROM org_pipelines_info"
    res1 <- setkey(data.table(DBI::dbGetQuery(cn, query_1)), org_uuid)
    res2 <- setkey(data.table(DBI::dbGetQuery(cn, query_2)), org_uuid)
    out <- res1[res2[current_client & in_population], .(org_uuid, org)]
    setkey(out, "org_uuid")[]
  }
  store_index()[org_index()]
}

## -----------------------------------------------------------------------------
dbLeaflogixCreds <- function() {
  cn <- dbc("prod2", "hcaconfig")
  on.exit(dbd(cn))
  qry <- "SELECT STORES.org_uuid, STORES.store_uuid, consumerkey, auth
          FROM (SELECT * FROM org_credentials_leaflogix) CREDS
          INNER JOIN (SELECT store_uuid, org_uuid FROM org_stores) STORES
          ON CREDS.store_uuid = STORES.store_uuid"
  setDT(DBI::dbGetQuery(cn, qry), key = c("org_uuid", "store_uuid"))[]
}

## -----------------------------------------------------------------------------
dbBrandPatterns <- function() {
  cn <- dbc("prod2", "consolidated")
  on.exit(dbd(cn))
  out <- setDT(DBI::dbReadTable(cn, "product_brands"))
  out[, brand_regex := stringr::str_to_upper(brand_regex)]
  out[]
}

## -----------------------------------------------------------------------------
active_clients <- dbOrgIndex()
pin_write(board, active_clients, type = "rds")

creds_leaflogix <- dbLeaflogixCreds()
pin_write(board, creds_leaflogix, type = "rds")

brand_patterns <- dbBrandPatterns()
pin_write(board, brand_patterns, type = "rds")

## ---- eval=FALSE--------------------------------------------------------------
#  board <- board_connect(server = apiUrl, key = apiKey)
#  pin_read(board, "bobbyf/active_org_locations")

