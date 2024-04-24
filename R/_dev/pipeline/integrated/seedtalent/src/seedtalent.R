box::use(
  dplyr[inner_join, rename, select],
  pipelinetools[db_read_table_unique]
)

# SEED TALENT DATA --------------------------------------------------------
build_seedtalent_courses <- function(pg) {
  courses <- db_read_table_unique(
    pg, "seedtalent_courses", "id", "run_date_utc",
    columns = c("id", "updated", "created", "reference", "description", "url", "name")
  ) |>
    select(
      course_id = id,
      course_updated = updated,
      course_created = created,
      course_ref = reference,
      course_desc = description,
      url,
      course_name = name
    )
  return(courses)
}

build_seedtalent_tests <- function(pg, org) {
  users <- db_read_table_unique(
    pg, paste0(org, "_seedtalent_users"), "id", "run_date_utc", columns = c(
      "id", "last_name", "first_name", "email", "pos", "pos_id"
    )
  ) |>
    rename(user_id = id, posId = pos_id)
  user_tests <- db_read_table_unique(
    pg, paste0(org, "_seedtalent_user_tests"), "id", "run_date_utc", columns = c(
      "id", "course_id", "user_id", "status", "progress", "duration", "score", "start", '"end"',
      "passed", "fail_count"
    )
  ) |>
    rename(failCount = fail_count)
  st_df <- inner_join(users, user_tests, by = "user_id")
  return(st_df)
}
