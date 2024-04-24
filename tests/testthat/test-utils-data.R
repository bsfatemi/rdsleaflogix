library(data.table)

test_that("Data utilities", {

  ##
  ## split by row
  ##
  DT <- setDT(copy(iris))
  ll <- split_by_row(DT)
  expect_true(is.list(ll))
  expect_true(DT[, .N] == length(ll))
  expect_identical(rbindlist(ll), DT)

  ##
  ## clean gender
  ##
  tmp <- c(NA, "Male", "male", "m", "M",  "MALE", "feMale", "F", "f", "RANDO")
  expect_identical(str_gender(tmp), c(NA, "M", "M", "M", "M", "M", "F", "F", "F", NA))
})


