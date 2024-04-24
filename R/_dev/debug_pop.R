library(data.table)
library(rdleaflogix)

## Execute cluster
fx <- function(x) {
  do.call(pl_population, x[, .(org, store, auth, ckey = consumerkey, write = FALSE)])
}

# argsll <- rev(split_by_row(get_leaflogix_index()))
# pll <- lapply(argsll, fx)



args <- get_leaflogix_index()[
  org == "jarsaz" & store == "dcenter",
  .(org, store, auth, ckey = consumerkey, write = FALSE)
]

a <- args
org <- a$org
store <- a$store
auth <- a$auth
ckey <- a$ckey
write <- a$write

nams <- args[, paste0(org, ".", store)]
argsll <- split_by_row(args)
pll <- setNames(lapply(seq_along(argsll), function(i) {
  print(i)
  a <- argsll[[i]]
  org <- a$org
  store <- a$store
  auth <- a$auth
  ckey <- a$ckey
  write <- a$write
  pl_population(org, store, auth, ckey, write)
}), nams)






pll <- run_pl_population(4, org = "jarsaz", LB = TRUE)
