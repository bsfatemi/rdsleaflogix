#' Utilities
#'
#' General utilities used by package functions
#'
#' @param DT A data table to be split into list row-wise
#' @param vec vector to clean
#'
#' @importFrom data.table transpose setnames setDT set
#' @importFrom rlang parse_expr
#' @importFrom stringr str_replace_na str_to_upper str_length
#'
#' @name utils-data
NULL


#' @describeIn utils-data split data.table row-wise into list
#' @export
split_by_row <- function(DT) {
  as_class <- lapply(DT, function(x) get(rlang::parse_expr(paste0("as.", class(x)))))
  ll <- transpose(as.list(DT), keep.names = "rn")
  lapply(ll[-1], function(x) {
    row_dt <- setnames(setDT(as.list(x)), ll[[1]])
    for (col in names(row_dt))
      set(row_dt, NULL, col, value = as_class[[col]](row_dt[, get(col)]))
    row_dt[]
  })
}

#' @describeIn utils-data clean gender column
#' @export
str_gender <- function(vec) {
  VEC <- stringr::str_replace_na(stringr::str_to_upper(as.character(vec)))
  VEC[stringr::str_length(VEC) == 0] <- "NA"
  VEC[stringr::str_detect(VEC, "^U.*$")] <- "NA"
  VEC[stringr::str_detect(VEC, "0")] <- "M"
  VEC[stringr::str_detect(VEC, "1")] <- "F"
  VEC[stringr::str_detect(VEC, "2")] <- "U"
  VEC[stringr::str_detect(VEC, "^F(EMALE)?$")] <- "F"
  VEC[stringr::str_detect(VEC, "^M(ALE)?$")] <- "M"
  VEC[stringr::str_detect(VEC, "^[MFU]$", negate = TRUE)] <- NA_character_
  return(VEC)
}
