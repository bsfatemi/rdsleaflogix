# Cabbage Patch Failed Pipelines Re-Run
#
# (C) 2020 Happy Cabbage Analytics Inc

box::use(
  parallel[detectCores, mclapply]
)

# Save the old WD, to restore it at the end.
old_wd <- getwd()
# If we are under hcapipelines path, keep it. If not, move to cypress default path.
if (basename(getwd()) != "hcapipelines") {
  setwd("~/hcapipelines")
}
source("inst/cron/run_and_log.R")
wd <- "./"

poss <- c("blaze", "leaflogix", "treez")
mclapply(poss, function(pos) {
  run_and_log(
    pipeline = "CABBAGE PATCH RE-RUNS",
    wd = wd,
    path = paste0("inst/pipeline/cabbage_patch/rerun/src/", pos, ".R")
  )
}, mc.cores = min(detectCores() - 1, length(poss)))

# Restore working directory.
setwd(old_wd)
