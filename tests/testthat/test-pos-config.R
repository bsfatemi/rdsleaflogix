


endpts <- .pos_config$leaflogix$endpoints
env <- rlang::pkg_env("rdleaflogix")

test_that("Checking exported functions", {

  ## check if there's a valid ext function for every get fun
  get_fun_names <- ls(envir = env, pattern = "get_ll_")
  ext_fun_names <- ls(envir = env, pattern = "ext_ll_")
  bld_fun_names <- ls(envir = env, pattern = "bld_ll_")
  wrt_fun_names <- ls(envir = env, pattern = "wrt_ll_")

  expect_identical(
    sort(stringr::str_remove(get_fun_names, "get_ll_")),
    sort(stringr::str_remove(ext_fun_names, "ext_ll_"))
  )

  expect_identical(
    sort(stringr::str_remove(ext_fun_names, "ext_ll_")),
    sort(stringr::str_remove(bld_fun_names, "bld_ll_"))
  )

  expect_identical(
    sort(stringr::str_remove(bld_fun_names, "bld_ll_")),
    sort(stringr::str_remove(wrt_fun_names, "wrt_ll_"))
  )


  ##
  ## Check that lookupIO is configured correctly to map:
  ##    - endpoints to get/ext
  ##    - inputs to outputs
  ##    - outputs to bld/wrt
  ##
  ll <- sapply(endpts, function(x) rdleaflogix:::lookupIO("leaflogix", x)[-1])

  for (i in ll)
    expect_true(!is.null(i))

  expect_identical(sort(names(ll)), sort(endpts))

  expect_identical(sort(names(ll)), sort(endpts))

  expect_identical(
    sort(endpts),
    sort(unique(stringr::str_remove(names(unlist(sapply(ll, unlist))), "(?=\\.).+")))
  )

  expect_error(lookupIO("hello", "world"), "POS Not Configured")
})










