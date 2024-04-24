library(data.table)

IN <- copy(rdleaflogix:::.pos_data$leaflogix$extract_index)


test_that("Testing build util", {
  expect_identical(check_index(IN), IN)
  expect_error(check_index(IN[, .(org)]), "pipeline input data is missing expected columns")

  DT <- check_index(IN)["brands", rbindlist(data)]
  expect_equal(DT, check_schema(DT, "leaflogix", "Brand"))
  expect_error(check_schema(DT, "a", "b"), "Schema not found")
  expect_null(check_schema(DT, "leaflogix", "ProductCategory"))

  res <- set_location_uuids(data.table(org = "medithrive", store = "main"))
  expect_true(is.data.table(res))
  expect_true(length(res) == 4)
  expect_named(res, c("org", "store", "org_uuid", "store_uuid"))
})

