## ----setup, include = FALSE---------------------------------------------------
library(rdleaflogix)
library(parallel)
library(data.table)
library(knitr)
library(pander)
library(stringr)

options(rmarkdown.html_vignette.check_title = FALSE)

Sys.setenv(PROJ_DIR = path.expand("~/PROJECTS"))


## ----make_cluster-------------------------------------------------------------
cl <- makeCluster(8, outfile = "cluster.log")

## ----make_job_id--------------------------------------------------------------

jobId <- as.integer(Sys.time())
clusterExport(cl, "jobId")

## ----init_node_environ--------------------------------------------------------
log_paths <- clusterEvalQ(cl, rdtools::open_log(paste0("pid-", Sys.getpid()), jobId))


oid <- hcaconfig::lookupOrgGuid("nobo")
args <- get_pipeline_args(pl = "leaflogix", org_uuids = oid)



