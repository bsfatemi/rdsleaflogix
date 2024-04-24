# Polaris App Data
# build_mstudio_sms_logs.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

build_mstudio_sms_logs <- function(df = NULL) {

  extracted <- df %>%
    filter(str_sub(response, 1, 1) == "{") %>%
    mutate(
      response = map(response, fromJSON, simplifyDataFrame = FALSE),
      http_status = map(http_status, fromJSON, simplifyDataFrame = FALSE),
      campaign_id = as.character(campaign_id)
    ) %>%
    unnest_wider("http_status", names_sep = "_") %>%
    unnest_wider("response") %>%
    unnest_wider("data",      names_sep = "_") %>%
    unnest_wider("data_to",   names_sep = "_") %>%
    unnest_wider("data_to_1", names_sep = "_") %>%
    unnest_wider("data_from", names_sep = "_") %>%
    unnest_wider("data_cost", names_sep = "_")

  if ("errors" %in% names(extracted)) {
    extracted[["data_errors"]] <- extracted[["errors"]]
    extracted[["errors"]] <- NULL
    extracted <- extracted %>%
      unnest_wider("data_errors", names_sep = "_") %>%
      unnest_wider("data_errors_1", names_sep = "_") %>%
      unnest_wider("data_errors_1_meta", names_sep = "_")
  }

  output <- mutate_if(extracted, is.list, ~ map_chr(., toJSON, force = TRUE))
  return(output)
}
