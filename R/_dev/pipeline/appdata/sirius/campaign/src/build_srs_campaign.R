build_srs_campaign <- function(raw) {
  orgs <- hcaconfig::orgIndex()
  orgs$org <- orgs$short_name
  orgs <- orgs[, org, org_uuid]

  data <- raw %>%
    filter(!is_single_text, !is_many_days) %>%
    left_join(orgs) %>%
    mutate(
      filters_applied = map_if(
        filters_applied, ~ !is.na(.x), ~ fromJSON(.x, simplifyDataFrame = FALSE)
      ),
      mms_included = if_else(is_mms, "Yes", "No"),
    ) %>%
    hoist(filters_applied, title = "title") %>%
    select(-filters_applied, -org) %>%
    mutate(
      grouped_source = case_when(
        str_detect(coalesce(title, campaign_source), "Promote") ~ "Promoted Categories",
        str_detect(coalesce(title, campaign_source), "Lost") ~ "Lost Customer Outreach",
        str_detect(coalesce(title, campaign_source), "New Customers") ~ "New Engagements",
        str_detect(coalesce(title, campaign_source), "Custom") ~ "Custom Segment",
        TRUE ~ "Other Key Opportunity"
      )
    )

  return(data)
}
