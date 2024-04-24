#' API Resources
#'
#' @param pl pipeline name
#' @param name endpoint or resource name
#'
#' @importFrom jsonlite fromJSON
#'
#' @name utils-api
#' @examples
#' get_pos_schema("leaflogix", "Customer")
NULL


#' @describeIn utils-api Get the schema for an endpoint dataset from the api
#'
#' @examples
#' # names can be found at api.pos.dutchie.com/swagger/index.html
#' get_pos_schema("leaflogix", "LoyaltySnapshot")
#'
#' @export
get_pos_schema <- function(pl, name) {
  url <- 'https://api.pos.dutchie.com/swagger/v001/swagger.json'
  ll <- jsonlite::fromJSON(..g(url))
  def <- ll$definitions[[name]]$properties

  skip_non_found <- which(sapply(def, function(x) is.null(x$type)))
  if (length(skip_non_found) > 0)
    def <- def[-skip_non_found]

  sapply(names(def), function(x) {
    switch(
      def[[x]]$type,
      string = "character",
      number = "numeric",
      integer = "integer",
      boolean = "logical",
      array = "list",
      `NULL` = NULL,
      x
    )
  }, simplify = FALSE)
}

