# VARS --------------------------------------------------------------------
org <- args$org
out_table <- paste(org, "klaviyo_members_events", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("members_events", org, "klaviyo")
# Remove errored queries.
json <- json[!sapply(json, is.null)]

# EXTRACT & WRITE ---------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
now_utc <- lubridate::now(tzone = "UTC")
# Extract and push members events, by chunks.
purrr::walk(split(json, ceiling(seq_along(json) / 5000)), function(json_chunk) {
  members_events <- hcaklaviyo::extract_profile_events_for_all_metrics(json_chunk)
  members_events$run_date_utc <- now_utc
  DBI::dbWriteTable(pg, out_table, members_events, append = TRUE)
})
hcaconfig::dbd(pg)
