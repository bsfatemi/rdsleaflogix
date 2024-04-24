extract_form_optins <- function(raw) {
  if (nrow(raw) == 0) {
    return(NULL)
  }

  # CLEAN COLUMN TITLES
  df <- raw %>%
    janitor::clean_names() %>%
    dplyr::select(
      "ts_gmt", "optin_id",
      dplyr::one_of("org", "orguuid", "custom_tags"),
      dplyr::starts_with("phone")
    )

  # IF ORG (short_name) IS PROVIDED, MAP IT TO ORGUUID
  indx <- dplyr::select(hcaconfig::orgIndex(), "org" = short_name, "orguuid" = org_uuid)
  if ("org" %in% names(df)) {
    if ("orguuid" %in% names(df)) {
      indx <- dplyr::rename(indx, newuuid = orguuid)
      df <- df %>%
        dplyr::left_join(indx) %>%
        dplyr::mutate(
          orguuid = dplyr::coalesce(orguuid, newuuid)
        ) %>%
        dplyr::select(-newuuid)
    } else {
      df <- dplyr::left_join(df, indx)
    }
  }

  # IF ORGUUID IS NOT IN DATA, RETURN NULL
  if (!"orguuid" %in% names(df)) {
    return(NULL)
  }

  # IF NONE OF COLUMNS ARE PHONE, RETURN NULL
  if (!any(stringr::str_detect(names(df), "phone"))) {
    return(NULL)
  }

  # COLLAPSE TO GET PHONE COLUMNS, CHECK FOR VALID DATA
  optins <- df %>%
    tidyr::pivot_longer(
      cols       = dplyr::starts_with("phone"),
      names_to   = "phoneCol",
      values_to  = "phone"
    ) %>%
    dplyr::mutate(
      phone = pipelinetools::process_phone(phone)
    ) %>%
    dplyr::filter(
      !is.na(phone), !is.na(orguuid)
    )

  # IF NO DATA LEFT RETURN NULL
  if (nrow(optins) == 0) {
    return(NULL)
  }

  form_optins <- optins %>%
    dplyr::select("ts_gmt", "optin_id", "orguuid", "phone", one_of("custom_tags"))

  return(form_optins)
}
