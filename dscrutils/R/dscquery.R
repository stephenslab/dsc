#' @title SQL-like interface for querying DSC output.
#'
#' @description This is an R interface to \code{dsc-query} for
#' conveniently extracting and exploring DSC results within the R
#' environment. For details, see the documentation for the
#' \code{dsc-query} command.
#' 
#' @param dsc.outdir Directory where DSC output is stored.
#' 
#' @param targets Query targets specified as a character vector, or,
#' character string separated by space, e.g.,
#' \code{targets = "simulate.n estimate mse.score"} and
#' \code{targets = c("simulate.n","estimate","mse.score")} are equivalent.
#' Using \code{paste}, eg \code{paste("simulate",c("n","p","df"),sep=".")}
#' one can specify multiple properties from the same module.
#' These will be the names of the columns in the returned data frame.
#'
#' @param conditions The default \code{NULL} means "no conditions", in
#' which case the results for all DSC pipelines are returned.
#'
#' @param groups Definition of module groups. This feature is not yet
#' implemented.
#' 
#' @param add.path If TRUE, the returned data frame will contain full
#' pathnames, not just the base filenames.
#' 
#' @param exec The command or pathname of the dsc-query executable.
#'
#' @param max.extract.vector Vector-valued DSC outputs not exceeding
#' this length are automatically extracted to the data frame.
#' 
#' @param verbose If \code{verbose = TRUE}, print progress of DSC
#' query command to the console.
#'
#' @return A data frame containing the result of the DSC query, with
#' columns corresponding to the query target. When reasonable to do
#' so, the DSC outputs are extracted into the columns of the data
#' frame; when the values are not extracted, the file names containing
#' the outputs are provided instead.
#'
#' Note that data frames cannot contain NULL values, and therefore
#' NULL-valued DSC outputs cannot be extracted into the data frame,
#' and must be loaded from the RDS files.
#'
#' @note We have made considerable effort to prevent column names from
#' being duplicated. However, we have not tested this extensively for
#' possible column name conflicts.
#'
#' This function may not work in Windows.
#'
#' @examples
#'
#' # Retrieve results from the "one_sample_location" DSC experiment in
#' # which the true mean is 1. The MSE (mean squared error) values
#' # should be extracted into the "mse.mse" column.
#' dsc.dir <- system.file("datafiles","one_sample_location",
#'                        "dsc_result",package = "dscrutils")
#' dat <- dscquery(dsc.dir,targets = "simulate.n estimate mse.mse",
#'                 condition = "simulate.true_mean = 1")
#' print(dat)
#'
#' # Retrieve some results from the "ash" DSC experiment. In this
#' # example, the beta estimates are long vectors (length 1,000), so the
#' # results are not extracted into the outputted data frame.
#' dsc.dir2 <- system.file("datafiles","ash","dsc_result",
#'                         package = "dscrutils")
#' dat2 <-
#'   dscquery(dsc.dir2,
#'            targets = c(paste("simulate",c("nsamp","g"),sep="."),
#'                        paste("shrink",c("mixcompdist","beta_est","pi0_est"),
#'                              sep=".")),
#'            condition = paste("simulate.g =",
#'                              "'ashr::normalmix(c(2/3,1/3),c(0,0),c(1,2))'"))
#'
#' # This is the same as the previous example, but extracts the
#' # vector-valued beta estimates into the outputted data frame. As a
#' # result, the data frame of query results is much larger (it has over
#' # 1000 columns).
#' 
#' dat3 <-
#'   dscquery(dsc.dir2,
#'            targets = c("simulate.nsamp","simulate.g","shrink.mixcompdist",
#'                        "shrink.beta_est","shrink.pi0_est"),
#'            condition = paste("simulate.g =",
#'                              "'ashr::normalmix(c(2/3,1/3),c(0,0),c(1,2))'"),
#'            max.extract.vector = 1000)
#' 
#' # This query should generate an error because there is no output
#' # called "score" in the "mse" module.
#'
#' \dontrun{
#' dat4 <- dscquery(dsc.dir,targets = c("simulate.n","estimate","mse.score"),
#'                  condition = "simulate.true_mean = 1")
#' }
#' 
#' @importFrom utils read.csv
#' 
#' @export
dscquery <- function (dsc.outdir, targets, conditions = NULL, groups,
                      add.path = FALSE, exec = "dsc-query",
                      max.extract.vector = 10, verbose = TRUE) {

  # CHECK INPUTS
  # ------------
  # Check input argument "dsc.outdir".
  if (!(is.character(dsc.outdir) & length(dsc.outdir) == 1))
    stop("Argument \"dsc.outdir\" should be a character string")
    
  # Check input argument "targets".
  if (!(is.character(targets) & is.vector(targets)))
    stop("Argument \"targets\" should be a character vector")

  # Check input argument "conditions".
  if (!is.null(conditions))
    if (!(is.character(conditions) & length(conditions) == 1))
      stop("Argument \"conditions\" should be NULL or a character vector")
    
  # Check input argument "groups".
  if (!missing(groups))
    stop("Argument \"groups\" is not yet implemented")

  # Check input argument "add.path".
  if (!(is.logical(add.path) & length(add.path) == 1))
    stop("Argument \"add.path\" should be TRUE or FALSE")
  if (add.path)
    stop("\"add.path = TRUE\" not currently implemented")
    
  # Check input argument "exec".
  if (!(is.character(exec) & length(exec) == 1))
    stop("Argument \"exec\" should be a character string")

  # Check input argument "verbose".
  if (!(is.logical(verbose) & length(verbose) == 1))
    stop("Argument \"verbose\" should be TRUE or FALSE")

  # RUN DSC QUERY COMMAND
  # ---------------------
  # Generate a temporary directory where the query output will be
  # stored.
  outdir  <- file.path(tempdir(),"dsc")
  outfile <- file.path(outdir,"query.csv")
  dir.create(outdir,showWarnings = FALSE,recursive = TRUE)
  
  # If something fails in subsequent steps, delete the temporary
  # directory.
  on.exit(unlink(outdir,recursive = TRUE),add = TRUE)
  
  # Build the command based on the inputs.
  cmd.str <- paste(exec,dsc.outdir,"-o",outfile,
                   "--target",paste(targets,collapse = " "))
  if (length(conditions) > 1)
    conditions <- paste(conditions,collapse = " & ")
  if (!is.null(conditions))
    cmd.str <- paste0(cmd.str," --condition \"",conditions,"\"")
  if (verbose) {
    cat("Running shell command:\n")
    cat(cmd.str,"\n")
  }
  out <- system(cmd.str,ignore.stdout = !verbose,ignore.stderr = !verbose)
  if (out != 0)
    stop("dsc-query command failed (returned a non-zero exit status)")
  
  # LOAD DSC QUERY
  # --------------
  if (verbose)
    cat("Loading dsc-query output from CSV file.\n")
  dat <- read.csv(outfile,header = TRUE,stringsAsFactors = FALSE,
                  check.names = FALSE,comment.char = "",
                  na.strings = "")
  n   <- nrow(dat)
  dat <- as.list(dat)
  for (i in 1:length(dat)) {
    dat[[i]]        <- data.frame(dat[[i]])
    names(dat[[i]]) <- names(dat)[i]
  }
      
  # PROCESS THE DSC QUERY RESULT
  # ----------------------------
  # Get all the columns of the form "module.variable.output".
  cols <- which(sapply(as.list(names(dat)),function (x) {
    n <- nchar(x)
    if (n < 7 | length(unlist(strsplit(x,"[:]"))) != 2)
      return(FALSE)
    else
      return(substr(x,n-6,n) == ":output")
  }))
  
  # Repeat for each column of the form "module.variable.output".
  if (length(cols) > 0) {
    cat("Reading DSC outputs:\n")
    for (j in cols) {

      # Get the column name (col), module name (module), variable name
      # (var) and new column name (col.new).
      col     <- names(dat)[j]
      x       <- unlist(strsplit(col,"[.]"))
      module  <- x[1]
      var     <- substr(x[2], 1, nchar(x[2])-7)
      col.new <- paste(module,var,sep = ".")
      if (verbose)
        cat(" - ",col.new,": ",sep = "")

      # This list will contain the value of the variable for each table row.
      values <- vector("list",n)
      
      # Extract the value of the selected output from each of the RDS
      # files. Repeat for each row of the query table.
      dsc.module.files <- dat[[j]][[1]]
      for (i in 1:n) {
        dscfile <- file.path(dsc.outdir,paste0(dsc.module.files[i],".rds"))
        out     <- readRDS(dscfile)

        # Check that the variable is one of the outputs in the file.
        if (!is.element(var,names(out)))
          stop(paste0("Output \"",var,"\" unavailable in ",dscfile))

        # Extract the value of the variable.
        values[[i]] <- out[[var]]
      }

      # If all the values are atomic, not NULL, and scalar (i.e.,
      # length of 1), then the values can fit into the column of a data
      # frame. If not, then there is nothing to be done.
      if (all(sapply(values,function (x) !is.null(x) &
                                         is.atomic(x) &
                                         length(x) == 1))) {
        if (verbose)
          cat("extracted atomic values\n")
        dat[[j]]        <- data.frame(unlist(values))
        names(dat[[j]]) <- col.new
        names(dat)[j]   <- col.new
      } else {

        # If (1) all the values are vectors, (2) the vectors are of
        # the same length, and (3) the vector lengths to not exceed
        # the maximum allowed vector length, then incorporate the
        # vector values into the data frame.
        extract.values <- FALSE
        if (all(sapply(values,is.vector)))
          if (length(unique(sapply(values,length))) == 1 &
              max(sapply(values,length)) <= max.extract.vector)
            extract.values <- TRUE
        if (extract.values) {
          if (verbose)
            cat("extracted vector values\n")
          dat[[j]] <- data.frame(do.call(rbind,values),
                                 check.names = FALSE,
                                 stringsAsFactors = FALSE)
          names(dat[[j]]) <- paste(col.new,1:ncol(dat[[j]]),sep = ".")
        } else if (verbose)
          cat("not extracted (filenames provided)\n")
      }
    }
  }
  
  # Output the query result as a data frame.
  dat.names  <- unlist(lapply(dat,names))
  dat        <- do.call(cbind,dat)
  names(dat) <- dat.names
  return(dat)
}
