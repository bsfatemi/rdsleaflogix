extract_internal_logs <- function(path) {
  files <- list.files(path, recursive = TRUE, full.names = TRUE, pattern = "*.fst")
  jsons <- purrr::map(files, readRDS)
  ll <- purrr::map(jsons, jsonlite::fromJSON, simplifyDataFrame = FALSE)

  df <- dplyr::tibble(list = ll) %>%
    tidyr::unnest_wider(list) %>%
    tidyr::unnest_wider(sys_info, names_sep = "_")
  df$pl_args <- purrr::map_chr(df$pl_args, ~ jsonlite::toJSON(.x, auto_unbox = TRUE))

  cln <- df %>%
    dplyr::filter(!is.na(id)) %>%
    dplyr::filter(!is.na(sys_info_sysname))

  return(cln)
}
