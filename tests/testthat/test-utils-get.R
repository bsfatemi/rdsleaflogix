library(data.table)


test_that("Testing get util - read_leaflogix_creds()", {
  DT <- read_leaflogix_creds()
  c_names <- c("org_uuid", "store_uuid", "consumerkey", "auth")
  expect_named(DT, c_names)
  expect_true(nrow(DT) > 10)

  expect_error(read_leaflogix_creds(pl = "blah"))
})


test_that("Testing get util - get_pipeline_args()", {
  expect_error(get_pipeline_args(pl = "blah"))

  ll <- get_pipeline_args(pl = "leaflogix")

  expect_true(is.list(ll))
  expect_true(all(sapply(ll, is.data.table)))

  for (i in ll)
    expect_named(i, c("org", "store", "auth", "consumerkey", "tab"), ignore.order = TRUE)

  org_ids <- c("aef30ad4-f4d9-4093-a569-b9c55c56844b",
               "292d1559-da00-4244-b392-5a9d70b9b3ee",
               "f38cd5d0-f1e2-475c-ac14-d70d1595abb4",
               "e491c84c-cbaa-4d24-b18c-3a5847aae335",
               "273405b6-b002-4c9a-8a26-1e856f67a681",
               "invalid junk here")
  ck_orgs <- rbindlist(get_pipeline_args("leaflogix", org_ids))[, unique(org)]
  expect_true(length(ck_orgs) < 5)
  expect_error(get_pipeline_args("leaflogix", "273405b6-b002-4c9a-8a26-1e856f67a681"))
})


test_that("Testing get util - get_org_index()", {
  expect_true(is.data.table(read_org_index()))
})







