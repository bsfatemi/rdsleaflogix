FROM ubuntu:20.04
RUN apt update
ENV ORG=verano
ENV STORE=meriden
ENV CKEY=
ENV AUTH=
ENV TZ=US/Pacific
ENV GITHUB_PAT=
ENV HCA_DB_PWD=
ENV CONNECT_SERVER=
ENV CONNECT_API_KEY=
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt install -y build-essential dirmngr gnupg apt-transport-https ca-certificates software-properties-common
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/'
RUN apt update && apt upgrade -y
RUN apt install r-base -y
RUN Rscript -e 'install.packages("remotes")'
RUN Rscript -e 'remotes::install_github("r-data-science/rdtools")'
RUN apt install -y libcurl4-openssl-dev libicu-dev libpq-dev libssl-dev make zlib1g-dev
RUN Rscript -e 'remotes::install_github("r-data-science/rdleaflogix")'
RUN Rscript -e 'cat("\n");rdtools::log_suc("Docker build complete...");cat("\n\n")'
RUN Rscript -e 'rdleaflogix::run_population(Sys.getenv("ORG"), Sys.getenv("STORE"), Sys.getenv("AUTH"), Sys.getenv("CKEY"))'
