test_that("Api data definitions", {
  ll <- get_pos_schema("leaflogix", "Customer")
  expect_true(is.list(ll) & length(ll) > 1)
})

