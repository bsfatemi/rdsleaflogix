library(data.table)

expect_data.table <- function(v, check_names = NULL) {
  expect_true(is.data.table(v), label = "is data.table")
  expect_true(nrow(v) > 0, label = "has rows")
  expect_true(length(v) > 0, label = "has columns")
  if (!is.null(check_names))
    expect_true(all(check_names %in% names(v)), label = "has names")
  invisible(v)
}

funs <- sapply(get_ep_names("leaflogix"), lookup_pl_funs, simplify = FALSE)

a <- .pos_data$leaflogix$pipeline_args[[6]]

test_that("Leaflogix Endpoint get/extract/build", {
  endpts <- get_ep_names("leaflogix")

  for (tab in endpts) {
    ..get <- funs[[tab]]$get[[tab]]
    ..ext <- funs[[tab]]$ext[[tab]]
    ..bld <- funs[[tab]]$bld[[tab]]

    ll <- ..get(a$org, a$store, a$auth, a$consumerkey) |>
    ..ext(org = a$org, store = a$store) |>
    ..bld(org = a$org, store = a$store)

    expect_true(length(ll) >= 1)

    for (dt in ll)
      expect_data.table(dt)
  }
})

test_that("Leaflogix Population get/extract/build", {
  org <- a$org
  store <- a$store
  auth <- a$auth
  consumerkey <- a$consumerkey

  ll <- get_ll_population(org, store, auth, consumerkey, n = 2) |>
    ext_ll_population(org, store) |>
    bld_ll_population(org, store)

  expect_true(length(ll) >= 1)

  for (dt in ll)
    expect_data.table(dt)
})


# a <- .pos_data$leaflogix$pipeline_args[[3]]

