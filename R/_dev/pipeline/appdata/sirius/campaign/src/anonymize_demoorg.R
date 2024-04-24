box::use(
  dplyr[filter, if_else, left_join, mutate, pull, select],
  hcaconfig[lookupOrgGuid],
  janeaustenr[austen_books],
  stringi[stri_reverse],
  stringr[str_to_sentence]
)

# Anonymize Sirius appdata.
anonymize_demoorg <- function(demoorg_sad, new_org_name, demoorg_campaigns) {
  demo_sample <- austen_books() |>
    filter(text != "", nchar(text) > 50) |>
    pull(text) |>
    str_to_sentence()
  # Attempt to give the same `body` to each campaign, throughout every demoorg pipeline.
  set.seed(420)
  demoorg_campaigns <- mutate(
    demoorg_campaigns,
    campaign_id = stri_reverse(campaign_id),
    body = paste(
      sample(demo_sample, size = nrow(demoorg_campaigns), replace = TRUE),
      sample(demo_sample, size = nrow(demoorg_campaigns), replace = TRUE)
    )
  ) |>
    select(campaign_id, body)
  select(demoorg_sad, -body) |>
    mutate(campaign_id = stri_reverse(campaign_id)) |>
    left_join(demoorg_campaigns, by = "campaign_id") |>
    mutate(
      # Replace media_url by a random (changing) picture.
      media_url = if_else(is.na(media_url), media_url, "https://picsum.photos/400/600"),
      org_uuid = lookupOrgGuid(new_org_name)
    )
}
