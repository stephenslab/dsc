#' @title SQL-like interface for querying DSC output.
#'
#' @description Add more detailed (paragraph-length) description here.
#' 
#' @param dsc.outdir Directory where DSC output is stored.
#' 
#' @param targets Query targets specified as a character vector, e.g.,
#' \code{targets = c("simulate.n","estimate","mse.score")}. These will
#' be the names of the columns in the returned data frame.
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
#' @param verbose If \code{verbose = TRUE}, print progress of DSC
#' query command to the console.
#'
#' @return A data frame containing the result of the DSC query, with
#' columns corresponding to the query target.
#'
#' @note May not work in Windows.
#'
#' @examples
#'
#' # Retrieve results from the "one_sample_location" experiment # in
#' # which the true mean is 1.
#' dsc.dir <- system.file("datafiles","one_sample_location",
#'                        "dsc_result",package = "dscrutils")
#' dat <- dscquery(dsc.dir,targets = c("simulate.n","estimate","mse.mse"),
#'                 condition = "simulate.true_mean = 1")
#' print(dat)
#'
#' # This query should generate an error because there is no output
#' called "score" in the "mse" module.
#' dat2 <- dscquery(dsc.dir,targets = c("simulate.n","estimate","mse.score"),
#'                  condition = "simulate.true_mean = 1")
#'
#' @export
dscquery <- function (dsc.outdir, targets, conditions = NULL, groups,
                      add.path = FALSE, exec = "dsc-query",
                      verbose = TRUE) {

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
  #
  # NOTE: It may be important at some point to use internal "absolute"
  # function from workflowr package to specify the pathnames.
  # 
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
                  check.names = FALSE,comment.char = "",na.strings = "")

  browser()
  
  # PROCESS THE DSC QUERY RESULT
  # ----------------------------
  # Get all the columns of the form "module.variable.output".
  cols <- unlist(lapply(as.list(names(dat)),function (x) {
    n <- nchar(x)
    if (n < 7 | length(unlist(strsplit(x,"[.]"))) != 3)
      return(NULL)
    else if (substr(x,n-6,n) != ".output")
      return(NULL)
    else
      return(substr(x,1,n-7))
  }))

  # Repeat for each column of the form "module.variable.output".
  if (length(cols) > 0)
    cat("Reading DSC output:\n")
  for (col in cols) {
    if (verbose)
      cat(" - ",col,":",sep = "")
  }
  
  # Output the query result stored in a data frame.
  return(dat)
}
