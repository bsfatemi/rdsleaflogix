#' Tools to Faciliate the Consolidation Step
#'
#' @param DT internal - advanced use only
#' @param index internal - advanced use only
#' @param pl internal - advanced use only
#' @param name internal - advanced use only
#' @param lmsg internal - advanced use only
#' @param include internal - advanced use only
#' @param ignore internal - advanced use only
#'
#'
#' @import data.table
#' @importFrom stringr str_glue str_flatten_comma str_remove str_squish str_to_upper str_extract str_remove_all str_detect
#' @importFrom rdtools log_err log_wrn log_inf log_suc
#'
#' @name utils-build
NULL


#' @describeIn utils-build internal function that appends org and store uuid to built data
set_location_uuids <- function(DT) {
  if ( !"org" %in% names(DT) || !"store" %in% names(DT) )
    stop("Data requires org and store names to join in uuids")
  setkey(DT, org, store)[
    setkey(read_org_index(), org, store),
    c("org_uuid", "store_uuid") := .(org_uuid, store_uuid)][]
}


#' @describeIn utils-build internal function that checks input
check_index <- function(index) {
  if (!all(c("org", "store", "name", "data") %in% names(index)))
    stop("pipeline input data is missing expected columns")
  if (!nrow(index) > 0)
    stop("pipeline input data is empty")
  if (any(is.na(index$org)) | any(is.na(index$store)))
    stop("Columns org or store have NA values")

  setkey(index, name)
  invisible(index)
}

#' @describeIn utils-build internal function for checking consolidated data against POS schema
check_schema <- function(DT, pl, name, lmsg = NULL, include = NULL, ignore = NULL) {

  ## if theres no rows or only two columns (org and store) then abort
  if (nrow(DT) == 0 || ncol(DT) == 2) {
    log_err(paste0(lmsg, "|...No data available to consolidate"))
    return(NULL)
  }

  lmsg[is.null(lmsg)] <- paste0(pl, "|", name) ## set default log message if NULL

  log_inf(paste0(lmsg, "|...Checking data against POS schema"))

  schema <- c(.pos_config[[pl]]$schema[[name]], list(org = "character", store = "character"))

  if (length(schema) == 2)
    stop("Schema not found")

  tmp <- unique(c(names(schema), include))
  expect_cols <- tmp[!tmp %in% ignore]
  actual_cols <- names(DT)

  nf_schema <- which(!expect_cols %in% actual_cols) # Expected but missing
  nf_dtable <- which(!actual_cols %in% expect_cols) # Found but not expected

  if (length(nf_schema) > 0) {
    cols <- stringr::str_flatten_comma(expect_cols[nf_schema])
    log_err(paste0(lmsg, "|...col(s) not found: ", cols))
    return(NULL)
  }

  if (length(nf_dtable) > 0) {
    cols <- stringr::str_flatten_comma(actual_cols[nf_dtable])
    log_wrn(paste0(lmsg, "|...dropping undefined col(s): ", cols))
  }
  DT[, expect_cols, with = FALSE]
}


#' @describeIn utils-build Build Category3 Mappings
set_category3 <- function(DT) {

  DT[, raw_category_name := stringr::str_to_lower(raw_category_name)]
  DT[, product_name := stringr::str_to_upper(product_name)]
  DT[, product_category_name := stringr::str_to_upper(product_category_name)]

  ##
  ## DRINKS
  ##
  DT[stringr::str_detect(raw_category_name, "drink|beverage"),
     category3 := "DRINKS"]
  DT[is.na(category3) & stringr::str_detect(product_name, "DRINK|BEVERAGE"),
     category3 := "DRINKS"]

  ##
  ## TABLETS AND CAPSULES
  ##
  DT[is.na(category3) & stringr::str_detect(raw_category_name, "tablet|pill|capsule"),
     category3 := "TABLETS_CAPSULES"]
  DT[is.na(category3) & stringr::str_detect(product_name, "TABLET|CAPSULE"),
     category3 := "TABLETS_CAPSULES"]

  ##
  ## EDIBLES
  ##
  pat <- paste0("(", paste(
    "(?<!non)(?<!non )(?<!non-)edible",
    "gummies|gummy|lozenge|chocolate|candy|brownies",
    "baked goods?",
    "chews",
    sep = ")|("
  ), ")")
  DT[is.na(category3) & stringr::str_detect(raw_category_name, pat),
     category3 := "EDIBLES"]
  DT[is.na(category3) & is.na(raw_category_name) &
       stringr::str_detect(product_name, "GUMMIES|CHEWS"),
     category3 := "EDIBLES"]

  ##
  ## VAPES AND BATTERIES
  ##
  DT[is.na(category3) &
       stringr::str_detect(raw_category_name, "pods|vape|cart|vapor|disposable|vaping"),
     category3 := "VAPES"]
  DT[is.na(category3) & stringr::str_detect(raw_category_name, "batteries|battery"),
     category3 := "BATTERIES"]
  DT[is.na(category3) & stringr::str_detect(product_name, "BATTER(Y|IES)"),
     category3 := "BATTERIES"]
  DT[is.na(category3) & is.na(raw_category_name) & stringr::str_detect(product_name, "CARTRIDGE"),
     category3 := "VAPES"]
  DT[is.na(category3) & stringr::str_detect(product_name, "C-CELL"),
     category3 := "VAPES"]

  ##
  ## PREROLLS
  ##
  DT[stringr::str_detect(raw_category_name, "pre( |\\-)?roll|joint|blunt"),
     category3 := "PREROLLS"]
  DT[is.na(category3) & stringr::str_detect(product_name, "PRE( |\\-)?ROLL"),
     category3 := "PREROLLS"]

  ##
  ## FLOWER
  ##
  # work manually with the word bud bc it appears in concentrates/extracts also.
  DT[raw_category_name %in% c("bud", "buds", "bulkbud", "packed bud", "packedbud"),
     category3 := "FLOWER"]
  DT[is.na(category3) & stringr::str_detect(raw_category_name, "flower"),
     category3 := "FLOWER"]
  DT[is.na(category3) & stringr::str_detect(product_name, "SHAKE"),
     category3 := "FLOWER"]

  pat <- paste0("(", paste(
    "pre-packaged|prepack|eighths", "((3\\.5|7|14)g special)|quarter|eighth",
    sep = ")|("
  ), ")")
  DT[is.na(category3) & stringr::str_detect(raw_category_name, pat),
     category3 := "FLOWER"]
  DT[is.na(category3) & stringr::str_detect(product_category_name, "FLOWER"),
     category3 := "FLOWER"]

  ##
  ## EXTRACTS
  ##
  pat <- "extract|concentrate|budder|shatter|badder|live resin|live rosin|wax|live diamonds|sauce"
  DT[is.na(category3) & stringr::str_detect(raw_category_name, pat),
     category3 := "EXTRACTS"]
  DT[is.na(category3) & stringr::str_detect(product_name, "SYRINGE"),
     category3 := "EXTRACTS"]
  DT[is.na(category3) & stringr::str_detect(raw_category_name, "distillate|applicator"),
     category3 := "EXTRACTS"]

  ##
  ## TINCTURES
  ##
  DT[is.na(category3) & stringr::str_detect(raw_category_name, "tincture"),
     category3 := "TINCTURES"]
  DT[is.na(category3) & is.na(raw_category_name) & stringr::str_detect(product_name, "TINCTURE"),
     category3 := "TINCTURES"]

  ##
  ## TOPICALS
  ##
  pat <- "topical|ointment|bath bomb|balm|bath salt"
  DT[is.na(category3) & stringr::str_detect(raw_category_name, pat),
     category3 := "TOPICALS"]
  DT[is.na(category3) & stringr::str_detect(product_name, "TRANSDERMAL|TOPICAL"),
     category3 := "TOPICALS"]
  DT[is.na(category3) & stringr::str_detect(product_category_name, "TRANSDERMAL|TOPICAL"),
     category3 := "TOPICALS"]



  ##
  ## ACCESSORIES - merch, clothing, non-cannabis (excluding batteries)
  ##
  pat <- "accessor|non( |\\-)?cannabis|merch|apparel|clothes|clothing|shirt"
  DT[is.na(category3) & stringr::str_detect(raw_category_name, pat),
     category3 := "ACCESSORIES"]

  ##
  ## Misc high prominence products
  ##
  DT[is.na(category3) & stringr::str_detect(product_name, "SYRUP"),
     category3 := "DRINKS"]
  DT[is.na(category3) & stringr::str_detect(raw_category_name, "crumble|rosin|sugar|diamonds"),
     category3 := "EXTRACTS"]
  DT[is.na(category3) &
       stringr::str_detect(raw_category_name, "(smalls|shake) (indica|hybrid|sativa)"),
     category3 := "FLOWER"]
  DT[is.na(category3) & stringr::str_detect(raw_category_name, "sublingual"),
     category3 := "TABLETS_CAPSULES"]
  DT[is.na(category3) & stringr::str_detect(raw_category_name, "pre pack (3\\.5|14)"),
     category3 := "FLOWER"]
  DT[is.na(category3) & stringr::str_detect(raw_category_name, "paraphernalia|gear"),
     category3 := "ACCESSORIES"]
  DT[is.na(category3) & stringr::str_detect(raw_category_name, "hash|kief|resin|solventless"),
     category3 := "EXTRACTS"]
  DT[is.na(category3) & stringr::str_detect(raw_category_name, "clone|plant"),
     category3 := "ACCESSORIES"]
  DT[is.na(category3) & stringr::str_detect(raw_category_name, "cream/lotion"),
     category3 := "TOPICALS"]
  DT[is.na(category3) & stringr::str_detect(raw_category_name, "pod|vaping"),
     category3 := "VAPES"]
  DT[is.na(category3) & stringr::str_detect(product_name, "flower"),
     category3 := "FLOWER"]
  DT[is.na(category3) & stringr::str_detect(product_name, "cart|disposable"),
     category3 := "VAPES"]

  # DT[is.na(category3), category3 := "OTHER"]

  # Assign BATTERIES as ACCESSORIES.
  DT[category3 == "BATTERIES", category3 := "ACCESSORIES"]

  return(invisible(DT[]))
}

#' @describeIn utils-build standardize brands
set_brands <- function(DT) {
  DT$raw_brand_name <- DT$brand <- DT[, brand_name]
  DT$raw_product_name <- DT$product_name <- DT[, product_name]

  ## Set brand name to product name when NA
  DT[is.na(brand_name) & !is.na(product_name), brand_name := product_name]

  ## standardize and remove leading character
  DT[, brand_name := str_remove(str_squish(str_to_upper(brand_name)), "^[A-Z] ")]

  ## What does this do?
  DT[str_detect(brand_name, " \\(1/[248]\\)"),
     brand_name := str_extract(brand_name, ".+(?= \\(1/[248]\\))")]

  ## What does this do?
  DT[, brand_name := str_remove_all(brand_name, "\"|\\.|'")]

  ## get existing product brand mapping table
  product_brands <- read_brand_patterns()

  ## Iterate through brand regex and brand name mapping
  for (i in seq_len(nrow(product_brands))) {
    mp <- product_brands[i, ]
    DT[str_detect(brand_name, mp$brand_regex), brand_name := mp$brand_name]
  }

  ## Map all empty strings to HOUSE brand... WHY?
  DT[str_detect(brand_name, "^\\$"), brand_name := "HOUSE"]
  DT
}


