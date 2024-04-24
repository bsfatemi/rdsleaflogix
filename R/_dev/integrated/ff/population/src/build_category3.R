box::use(
  data.table[`:=`, as.data.table, copy],
  DBI[dbAppendTable, dbGetQuery, dbRemoveTable, dbWriteTable],
  dplyr[anti_join, bind_rows, left_join],
  logger[log_info]
)

na_sql_value <- "<NA_VALUE>"

#' Build Category3
#'
#' @param in_data A data.frame with columns `raw_category_name` and `product_name` for which to
#'   build category3 mappings.
#' @param conn A database connection to `consolidated`, to read and write mappings. If `NULL`,
#'   all mappings will be calculated, and not pushed to the database.
#'
#' @export
#'
build_category3 <- function(in_data, conn = NULL) {
  log_info("Cleaning raw category names and product names")
  # Copy as data.table the needed columns.
  in_data_dt <- copy(as.data.table(in_data))[, c("raw_category_name", "product_name")]
  # Edits to make mapping consistent.
  in_data_dt[, product_name := toupper(product_name)]
  in_data_dt[, raw_category_name := toupper(raw_category_name)]
  in_data_dt[raw_category_name == "", raw_category_name := NA]

  log_info("Getting existing/saved mappings.")
  old_mappings <- get_category3_mappings(in_data_dt, conn)
  log_info("Found ", nrow(old_mappings), " mappings.")
  # Get which rows still need to get mapped.
  missing_mapping <- anti_join(
    in_data_dt, old_mappings,
    by = c("raw_category_name", "product_name")
  )
  log_info("Calculating the new mappings.")
  new_mappings <- build_category3_mappings(missing_mapping, conn)
  log_info("Calculated ", nrow(new_mappings), " new mappings.")
  # Get all the mappings, and use them.
  all_mappings <- bind_rows(new_mappings, old_mappings)
  in_data_dt <- left_join(in_data_dt, all_mappings, by = c("raw_category_name", "product_name"))
  levs <- c(
    "FLOWER", "PREROLLS", "VAPES", "EXTRACTS", "EDIBLES", "DRINKS", "TABLETS_CAPSULES",
    "TINCTURES", "TOPICALS", "ACCESSORIES", "OTHER"
  )
  return(factor(in_data_dt$category3, levels = levs))
}

#' Get Category3 Mappings
#'
#' @param in_data_dt A data.table with columns `raw_category_name` and `product_name` for which to
#'   get category3 mappings.
#' @param conn A database connection to `consolidated`, to read mappings. If `NULL`, no mappings
#'   will be returned.
#'
get_category3_mappings <- function(in_data_dt, conn = NULL) {
  if (is.null(conn)) {
    return(data.frame(
      raw_category_name = character(0), product_name = character(0), category3 = character(0)
    ))
  }
  stopifnot(inherits(in_data_dt, "data.table"))
  in_data_dt <- unique(in_data_dt[, c("raw_category_name", "product_name")])
  # Replace NAs with a default value, to allow SQL INNER JOIN.
  in_data_dt[is.na(raw_category_name), raw_category_name := na_sql_value]
  in_data_dt[is.na(product_name), product_name := na_sql_value]
  # Try to get saved mappings for as many rows as possible.
  dbWriteTable(conn, "get_category3_mappings", in_data_dt, temporary = TRUE)
  mappings <- as.data.table(dbGetQuery(conn, "
    SELECT DISTINCT cm.raw_category_name, cm.product_name, category3
    FROM (
      category3_mappings cm
      INNER JOIN
      get_category3_mappings gcm
      ON
        (cm.raw_category_name = gcm.raw_category_name)
        AND (cm.product_name = gcm.product_name)
    )
  "))
  dbRemoveTable(conn, "get_category3_mappings")
  # Replace NA default values back to NAs.
  mappings[raw_category_name == na_sql_value, raw_category_name := NA_character_]
  mappings[product_name == na_sql_value, product_name := NA_character_]
  return(mappings)
}

#' Build Category3 Mappings
#'
#' It will return a data.table with unique columns `raw_category_name`, `product_name`, and
#' `category3`.
#'
#' @param in_data_dt A data.table with columns `raw_category_name` and `product_name` for which to
#'   build category3 mappings.
#' @param conn A database connection to `consolidated`, to push built mappings. If `NULL`, no
#'   mappings will be pushed.
#'
build_category3_mappings <- function(in_data_dt, conn = NULL) {
  ignore_case_grepl <- function(string, pattern) {
    grepl(pattern, string, ignore.case = TRUE, perl = TRUE)
  }

  stopifnot(inherits(in_data_dt, "data.table"))
  in_data_dt <- unique(in_data_dt[, c("raw_category_name", "product_name")])

  log_info("Mapping raw_category_name to drinks")
  ## DRINKS
  in_data_dt[ignore_case_grepl(raw_category_name, "drink|beverage"), category3 := "DRINKS"]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(product_name, "DRINK|BEVERAGE"), category3 := "DRINKS"
  ]

  log_info("Mapping raw_category_name to tablets and capsules")
  ## TABLETS AND CAPSULES
  in_data_dt[
    is.na(category3) & ignore_case_grepl(raw_category_name, "tablet|pill|capsule"),
    category3 := "TABLETS_CAPSULES"
  ]
  in_data_dt[ignore_case_grepl(product_name, "TABLET|CAPSULE"), category3 := "TABLETS_CAPSULES"]

  log_info("Mapping raw_category_name to edibles")
  ## EDIBLES
  pat <- paste0("(", paste(
    "(?<!non)(?<!non )(?<!non-)edible",
    "gummies|gummy|lozenge|chocolate|candy|brownies",
    "baked goods?",
    "chews",
    sep = ")|("
  ), ")")
  in_data_dt[is.na(category3) & ignore_case_grepl(raw_category_name, pat), category3 := "EDIBLES"]
  in_data_dt[
    is.na(category3) & is.na(raw_category_name) & ignore_case_grepl(product_name, "GUMMIES|CHEWS"),
    category3 := "EDIBLES"
  ]

  log_info("Mapping raw_category_name to vapes and batteries")
  ## VAPES AND BATTERIES
  in_data_dt[
    is.na(category3) &
      ignore_case_grepl(raw_category_name, "pods|vape|cart|vapor|disposable|vaping"),
    category3 := "VAPES"
  ]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(raw_category_name, "batteries|battery"),
    category3 := "BATTERIES"
  ]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(product_name, "BATTER(Y|IES)"), category3 := "BATTERIES"
  ]
  in_data_dt[
    is.na(category3) & is.na(raw_category_name) & ignore_case_grepl(product_name, "CARTRIDGE"),
    category3 := "VAPES"
  ]
  in_data_dt[is.na(category3) & ignore_case_grepl(product_name, "C-CELL"), category3 := "VAPES"]

  log_info("Mapping raw_category_name to prerolls")
  ## PREROLLS
  in_data_dt[
    ignore_case_grepl(raw_category_name, "pre( |\\-)?roll|joint|blunt"), category3 := "PREROLLS"
  ]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(product_name, "PRE( |\\-)?ROLL"), category3 := "PREROLLS"
  ]

  log_info("Mapping raw_category_name to flower")
  ## FLOWER
  # work manually with the word bud bc it appears in concentrates/extracts also.
  in_data_dt[raw_category_name %in% c("bud", "buds", "bulkbud", "packed bud", "packedbud"),
             category3 := "FLOWER"]

  in_data_dt[
    is.na(category3) & ignore_case_grepl(raw_category_name, "flower"), category3 := "FLOWER"
  ]
  in_data_dt[is.na(category3) & ignore_case_grepl(product_name, "SHAKE"), category3 := "FLOWER"]

  pat <- paste0("(", paste(
    "pre-packaged|prepack|eighths",
    "((3\\.5|7|14)g special)|quarter|eighth",
    sep = ")|("
  ), ")")
  in_data_dt[is.na(category3) & ignore_case_grepl(raw_category_name, pat), category3 := "FLOWER"]

  log_info("Mapping raw_category_name to extracts")
  ## EXTRACTS
  pat <- "extract|concentrate|budder|shatter|badder|live resin|live rosin|wax|live diamonds|sauce"
  in_data_dt[is.na(category3) & ignore_case_grepl(raw_category_name, pat), category3 := "EXTRACTS"]
  in_data_dt[is.na(category3) & ignore_case_grepl(product_name, "SYRINGE"), category3 := "EXTRACTS"]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(raw_category_name, "distillate|applicator"),
    category3 := "EXTRACTS"
  ]

  log_info("Mapping raw_category_name to tinctures")
  ## TINCTURES
  in_data_dt[
    is.na(category3) & ignore_case_grepl(raw_category_name, "tincture"), category3 := "TINCTURES"
  ]
  in_data_dt[
    is.na(category3) & is.na(raw_category_name) & ignore_case_grepl(product_name, "TINCTURE"),
    category3 := "TINCTURES"
  ]

  log_info("Mapping raw_category_name to topicals")
  ## TOPICALS
  pat <- "topical|ointment|bath bomb|balm|bath salt"
  in_data_dt[is.na(category3) & ignore_case_grepl(raw_category_name, pat), category3 := "TOPICALS"]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(product_name, "TRANSDERMAL|TOPICAL"),
    category3 := "TOPICALS"
  ]

  log_info("Mapping raw_category_name to accessories")
  ## ACCESSORIES - merch, clothing, non-cannabis (excluding batteries)
  pat <- "accessor|non( |\\-)?cannabis|merch|apparel|clothes|clothing|shirt"
  in_data_dt[
    is.na(category3) & ignore_case_grepl(raw_category_name, pat), category3 := "ACCESSORIES"
  ]

  log_info("Last mapping of raw_category_name to category3 for different categories.")
  ## Misc high prominence products
  in_data_dt[is.na(category3) & ignore_case_grepl(product_name, "SYRUP"), category3 := "DRINKS"]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(raw_category_name, "crumble|rosin|sugar|diamonds"),
    category3 := "EXTRACTS"
  ]
  in_data_dt[
    is.na(category3) &
      ignore_case_grepl(raw_category_name, "(smalls|shake) (indica|hybrid|sativa)"),
    category3 := "FLOWER"
  ]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(raw_category_name, "sublingual"),
    category3 := "TABLETS_CAPSULES"
  ]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(raw_category_name, "pre pack (3\\.5|14)"),
    category3 := "FLOWER"
  ]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(raw_category_name, "paraphernalia|gear"),
    category3 := "ACCESSORIES"
  ]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(raw_category_name, "hash|kief|resin|solventless"),
    category3 := "EXTRACTS"
  ]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(raw_category_name, "clone|plant"),
    category3 := "ACCESSORIES"
  ]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(raw_category_name, "cream/lotion"), category3 := "TOPICALS"
  ]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(raw_category_name, "pod|vaping"), category3 := "VAPES"
  ]
  in_data_dt[is.na(category3) & ignore_case_grepl(product_name, "flower"), category3 := "FLOWER"]
  in_data_dt[
    is.na(category3) & ignore_case_grepl(product_name, "cart|disposable"), category3 := "VAPES"
  ]
  in_data_dt[is.na(category3), category3 := "OTHER"]
  # Assign BATTERIES as ACCESSORIES.
  in_data_dt[category3 == "BATTERIES", category3 := "ACCESSORIES"]

  log_info("Append new category3 mappings and denote NA data.")
  if (!is.null(conn)) {
    # Replace NAs with a default value, to allow SQL INNER JOIN.
    in_data_dt[is.na(raw_category_name), raw_category_name := na_sql_value]
    in_data_dt[is.na(product_name), product_name := na_sql_value]
    dbAppendTable(conn, "category3_mappings", in_data_dt)
    # Replace NA default values back to NAs.
    in_data_dt[raw_category_name == na_sql_value, raw_category_name := NA_character_]
    in_data_dt[product_name == na_sql_value, product_name := NA_character_]
  }
  return(in_data_dt)
}
