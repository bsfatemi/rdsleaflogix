box::use(
  dplyr[filter, if_else, left_join, mutate, pull, select],
  janeaustenr[austen_books],
  stringi[stri_reverse],
  stringr[str_to_sentence]
)

# Anonymize Polaris appdata.
anonymize_demoorg <- function(demoorg_pad, new_org_name, demoorg_campaigns) {
  demo_sample <- austen_books() |>
    filter(text != "", nchar(text) > 50) |>
    pull(text) |>
    str_to_sentence()
  # Attempt to give the same `body` to each campaign, throughout every demoorg pipeline.
  set.seed(420)
  demoorg_campaigns <- mutate(
    demoorg_campaigns,
    org = new_org_name,
    campaign_id = stri_reverse(campaign_id),
    campaign_body = paste(
      sample(demo_sample, size = nrow(demoorg_campaigns), replace = TRUE),
      sample(demo_sample, size = nrow(demoorg_campaigns), replace = TRUE)
    ),
    phones_json = "{}",
    # Replace media_url by a random (changing) picture.
    media_url = if_else(is.na(media_url), media_url, "https://picsum.photos/400/600"),
  )
  demoorg_pad <- select(demoorg_pad, -org, -body, -media_url) |>
    mutate(campaign_id = stri_reverse(campaign_id)) |>
    left_join(
      select(demoorg_campaigns, campaign_id, org, body = campaign_body, media_url),
      by = "campaign_id"
    )
  return(list(demoorg_pad = demoorg_pad, demoorg_campaigns = demoorg_campaigns))
}
