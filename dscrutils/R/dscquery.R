#' @title R interface for querying DSC output.
#'
#' @description This is an R interface to the \code{dsc-query} program
#' for conveniently extracting and exploring DSC results within the R
#' environment. For additional information, run
#' \code{system("dsc-query --help")}.
#'
#' @param dsc.outdir Directory where the DSC output is stored.
#'
#' @param targets Query targets specified as a character vector; for
#' example, \code{targets = c("simulate.n","analyze","score.error")}.
#' A query target may be a module, a module group, a module parameter,
#' or a module output. These are \emph{required} targets; that is, DSC
#' pipelines (i.e., rows of the returned data frame) in which one of
#' more of the targets are unassigned or missing (\code{NA}) will be
#' automatically removed. To allow for unassigned or missing values,
#' use argument \code{targets.notreq} instead. This input argument,
#' together with \code{targets.notreq}, specifies the \code{--target}
#' flag in the \code{dsc-query} call. At least one of \code{targets}
#' and \code{targets.notreq} must not be \code{NULL} or empty. Note
#' that, to easily specify multiple targets from the same module, we
#' recommend using \code{\link{paste}}; e.g., \code{paste("simulate",
#' c("n","p","df"),sep = ".")}. These targets will be the names of the
#' columns in the data frame if a data frame is returned, or the names
#' of the list elements if a list is returned.
#'
#' @param targets.notreq Non-required query targets; this is the same
#' as \code{targets}, except that unassigned or missing values are not
#' removed from the return value. This input argument, together with
#' \code{targets}, specifies the \code{--target} flag in the
#' \code{dsc-query} call. At least one of \code{targets} and
#' \code{targets.notreq} must not be \code{NULL} or empty.
#'
#' @param conditions Conditions used to filter DSC pipeline results;
#' rows in which one or more of the conditions evaluate to
#' \code{FALSE} or \code{NA} are removed from the output (this is
#' convention used by \code{\link{which}}). When \code{conditions =
#' NULL}, no additional filtering of DSC pipelines is
#' performed. Although results can always be filtered \emph{post hoc},
#' using \code{conditions} to filter can significantly speed up
#' queries when the DSC outputs are very large, as this will filter
#' results, whenever possible, \emph{before} they are loaded into
#' R. Query conditions are specified as R expressions, in which target
#' names are written as \code{$(...)}; for example, to request only
#' results in which the value of parameter \code{sigma} in module
#' \code{simulate} is greater than or equal to \code{0.1}, set
#' \code{conditions = "$(simulate.sigma) >= 0.1"} (see below for
#' additional examples). This input argument specifies the
#' \code{--condition} flag in the call to \code{dsc-query}.
#'
#' @param groups Defines module groups. This argument specifies the
#' \code{--groups} flag in the call to \code{dsc-query}. For example,
#' \code{groups = c("method: mean median", "score: abs_err sqrt_err")}
#' will define two module groups, \code{method} and \code{score}.
#'
#' @param omit.filenames When \code{omit.filenames = FALSE}, an
#' additional column (or list element) giving the name of the DSC
#' output file is provided for each query target that is a module or
#' module group. This is useful if you want to directly inspect the
#' stored results. Setting \code{omit.filenames = TRUE} supresses
#' these additional outputs.
#' 
#' @param ignore.missing.files If \code{ignore.missing.files = TRUE},
#' all DSC output files that are not found will have \code{NA} for the
#' file name; when extracting target outputs from files, any outputs
#' in which files are not found will have their value set to \code{NA}. If
#' \code{ignore.missing.files = FALSE}, \code{dscquery} will throw an
#' error whenever a missing file is encountered.
#'
#' @param exec The command or pathname of the \code{dsc-query}
#' executable.
#'
#' @param verbose If \code{verbose = TRUE}, print progress of DSC
#' query command to the console.
#'
#' @return A list or data frame containing the result of the DSC
#' query.
#' 
#' When possible, DSC outputs are extracted into the columns of the
#' data frame. When outputs one or more outputs are large or complex
#' objects, the output is a list, with list elements corresponding to
#' the query targets. Each top-level list element should have the same
#' length.
#'
#' Note that a list can be later converted to a data frame using
#' \code{\link{as.data.frame}}, or converted to a "tibble" using the
#' \code{\link[tibble]{as_tibble}} function from the tibble package,
#' or converted to many other data structures.
#'
#' All targets specified by the "targets" and "targets.notreq"
#' arguments, except for targets that are module names, should have
#' columns (or list elements) of the same name in the output.
#' Whenever a target of the form "x.y" is requested, where "x" is a
#' module group and "y" is a module parameter or output, an additional
#' output for the module group is automatically included.  Additional
#' outputs giving file names of the DSC results files are included for
#' all targets that are modules or module groups.
#'
#' When targets are unassigned, these are stored as missing values
#' (\code{NA}).
#'
#' @details A call to dscquery cannot include targets that involve
#' both a module, and a module group containing that module. For
#' example, setting \code{targets = c("mean.est","analyze")} will
#' generate an error if "mean" is a module, and it is a member of the
#' "analyze" module group.
#'
#' This function may not work in Windows.
#'
#' @examples
#'
#' # Retrieve the number of samples ("n") and error summary ("error")
#' # from all simulations in the "one_sample_location" DSC experiment.
#' dsc.dir <- system.file("datafiles","one_sample_location",
#'                        "dsc_result",package = "dscrutils")
#' dat1 <- dscquery(dsc.dir,targets = "simulate.n analyze score.error")
#'
#' # Retrieve the results only for simulations in which the "mean"
#' # module was run. Because this condition is about a module name, it
#' # is applied before loading the full set of results into R, so the
#' # filtering step can speed up the query when there are many
#' # simulation results.
#' dat2 <- dscquery(dsc.dir,targets = "simulate.n analyze score.error",
#'                  conditions = "$(analyze) == 'mean'")
#' 
#' # Return results only for simulations in which the error summary is
#' # greater than 0.25. This condition is applied after loading the full
#' # set of results into R, and so this sort of condition will not
#' # reduce the query runtime.
#' dat3 <- dscquery(dsc.dir,targets = "simulate.n analyze score.error",
#'                conditions = "$(score.error) > 0.25")
#'
#' # Retrieve the DSC results only for simulations in which the "mean"
#' # module was run, and which which the error summary is greater than
#' # 0.25. The conditions in this case are applied before and after
#' # loading results into R.
#' dat4 <- dscquery(dsc.dir,targets = "simulate.n analyze score.error",
#'                  conditions = c("$(score.error) > 0.25",
#'                                "$(analyze) == 'median'"))
#'
#' # Retrieve some results from the "ash" DSC experiment. In this
#' # example, the beta estimates are long vectors (length 1,000), so the
#' # results are not extracted into the outputted data frame.
#' dsc.dir2 <- system.file("datafiles","ash","dsc_result",
#'                         package = "dscrutils")
#' dat5 <-
#'   dscquery(dsc.dir2,
#'            targets = c(paste("simulate",c("nsamp","g"),sep="."),
#'                        paste("shrink",c("mixcompdist","beta_est","pi0_est"),
#'                              sep=".")),
#'            conditions = "$(simulate.g)=='list(c(2/3,1/3),c(0,0),c(1,2))'")
#'
#' # This is the same as the previous example, but extracts the
#' # vector-valued beta estimates into the outputted data frame. As a
#' # result, the data frame of query results is much larger (it has over
#' # 1000 columns).
#' dat6 <-
#'   dscquery(dsc.dir2,
#'            targets = c("simulate.nsamp","simulate.g","shrink.mixcompdist",
#'                        "shrink.beta_est","shrink.pi0_est"),
#'            conditions = "$(simulate.g)=='list(c(2/3,1/3),c(0,0),c(1,2))'",
#'            max.extract.vector = 1000)
#'
#' \dontrun{
#'
#' # This query should generate an error because there is no output
#' # called "mse" in the "score" module.
#' dat7 <- dscquery(dsc.dir,targets = c("simulate.n","analyze","score.mse"),
#'                  conditions = "$(simulate.n) > 10")
#'
#' }
#'
#' @importFrom utils read.csv
#'
#' @export
#'
dscquery <- function (dsc.outdir, targets = NULL, targets.notreq = NULL,
                      conditions = NULL, groups = NULL, 
                      ignore.missing.files = FALSE,
                      omit.filenames = FALSE, exec = "dsc-query",
                      verbose = TRUE) {

  # CHECK & PROCESS INPUTS
  # ----------------------
  # Check input argument "dsc.outdir".
  if (!(is.character(dsc.outdir) & length(dsc.outdir) == 1))
    stop("Argument \"dsc.outdir\" should be a character vector of length 1")

  # Check input arguments "targets" and "targets.notreq".
  if (!is.null(targets))
    if (!(is.character(targets) & is.vector(targets) & length(targets) > 0))
      stop(paste("Argument \"targets\" should be \"NULL\", or a character",
                 "vector with at least one element"))
  if (!is.null(targets.notreq))
    if (!(is.character(targets.notreq) & is.vector(targets.notreq) &
          length(targets.notreq) > 0))
      stop(paste("Argument \"targets.notreq\" should be \"NULL\", or a",
                 "character vector with at least one element"))
  if (length(c(targets,targets.notreq)) == 0)
    stop(paste("Arguments \"targets\" and \"targets.notreq\" must specify",
               "at least one name; they cannot both be \"NULL\""))
  if (length(intersect(targets,targets.notreq)) > 0)
    stop(paste("Names cannot be the mentioned in both \"targets\" and",
               "\"targets.notreq\""))
  
  # Check input argument "conditions".
  if (!is.null(conditions))
    if (!(is.character(conditions) & is.vector(conditions) &
          length(conditions) > 0))
      stop(paste("Argument \"conditions\" should be \"NULL\", or a character",
                 "vector with at least one element"))

  # Check input argument "groups".
  if (!is.null(groups))
    if (!(is.character(groups) & is.vector(groups) & length(groups) > 0))
      stop(paste("Argument \"groups\" should be \"NULL\", or a character",
                 "vector with at least one element"))
  
  # Check input argument "ignore.missing.files".
  if (!(is.logical(ignore.missing.files) & length(ignore.missing.files) == 1))
    stop("Argument \"ignore.missing.files\" should be TRUE or FALSE")
  
  # Check input argument "omit.filenames".
  if (!(is.logical(omit.filenames) & length(omit.filenames) == 1))
    stop("Argument \"omit.filenames\" should be TRUE or FALSE")
  
  # Check input argument "exec".
  if (!(is.character(exec) & length(exec) == 1))
    stop("Argument \"exec\" should be a character vector of length 1")

  # Check input argument "verbose".
  if (!(is.logical(verbose) & length(verbose) == 1))
    stop("Argument \"verbose\" should be TRUE or FALSE")

  # PROCESS CONDITIONS
  # ------------------
  # Find the targets that are mentioned in the condition expressions, and
  # replace all instances of $(x) with x in these expressions so that
  # they can be evaluated as R expressions.
  if (!is.null(conditions)) {
    n                 <- length(conditions)
    condition_targets <- vector("list",n)
    for (i in 1:n) {
      out <- process.query.condition(conditions[i])
      condition_targets[[i]] <- out$targets
      conditions[i]          <- out$condition
    }
  }

  # RUN DSC QUERY COMMAND
  # ---------------------
  # If one or more conditions are specified, run the dsc-query program
  # once *without* the conditions. This is done to find which data
  # will be retained for the final output.
  if (!is.null(conditions)) {

    # Run dsc-query.
    if (verbose)
      cat("Calling dsc-query with non-condition targets.\n")
    out     <- build.dscquery.call(c(targets,targets.notreq),groups,
                                   dsc.outdir,outfile,exec)
    outfile <- out$outfile
    cmd.str <- out$cmd.str
    run_cmd(cmd.str,ferr = ifelse(verbose,"",FALSE))

    # Read the column names from the result of running dsc-query. (By
    # setting nrows = 1, we don't read the full output.)
    dat <- read.csv(outfile,header = TRUE,stringsAsFactors = FALSE,
                    check.names = FALSE,comment.char = "",na.strings = "NA",
                    nrows = 1)
    if (any(duplicated(names(dat))))
      stop("One or more names in dsc-query output are the same")

    # Determine the names of the columns (or list elements) for the
    # final return value. This involves removing the ":output" suffix
    # whenever it appears.
    final_outputs <- names(dat)
    n <- length(final_outputs)
    for (i in 1:n)
      if (is.output.column(final_outputs[i])) {
        x <- final_outputs[i]
        final_outputs[i] <- substr(x,1,nchar(x) - 7)
      }
    
    # Add the targets appearing in the condition expressions to the
    # set of "non-required" targets.
    targets.notreq <- c(targets.notreq,setdiff(do.call(c,condition_targets),
                                               targets.notreq))
  }

  # Now we are ready to run dsc-query with all targets. Note that
  # although the dsc-query program has the option to pass in
  # conditions, this feature is not used here, as the queries in this
  # interface are specified as R expressions.
  if (verbose)
    cat("Calling dsc-query with all targets (condition and non-condition).\n")
  out     <- build.dscquery.call(c(targets,targets.notreq),groups,
                                 dsc.outdir,outfile,exec)
  outfile <- out$outfile
  cmd.str <- out$cmd.str
  run_cmd(cmd.str,ferr = ifelse(verbose,"",FALSE))
  
  # IMPORT DSC QUERY RESULTS
  # ------------------------
  # As a safeguard, we check for any duplicated column names, and if
  # there are any, we halt and report an error.
  if (verbose)
    cat("Importing dsc-query output.\n")
  dat <- read.csv(outfile,header = TRUE,stringsAsFactors = FALSE,
                  check.names = FALSE,comment.char = "",
                  na.strings = "NA")
  if (any(duplicated(names(dat))))
    stop("One or more names in dsc-query output are the same")
  
  # FILTER BY TARGETS
  # -----------------
  # Filter out rows in which one or more of the targets are unassigned
  # or missing.
  dat <- filter.by.query.targets(dat,targets)
  
  # PRE-FILTER BY CONDITIONS
  # ------------------------
  # Filter rows of the data frame by each condition. If one or more
  # targets is unavailable, the condition is not applied.
  if (!is.null(conditions)) {
    n <- length(conditions)
    for (i in 1:n)
      if (!is.empty.result(dat))
        dat <- filter.by.condition(dat,conditions[i],condition_targets[[i]])
  }

  # EXTRACT DSC OUTPUT
  # ------------------
  # Extract outputs from DSC files for all columns with names of the
  # form "module.variable:output". After this step, dat will become a
  # nested list, in which each element dat[[i]][j]] is the value of
  # target i in pipeline j.
  if (verbose)
    cat("Reading DSC outputs.\n")
  if (!is.empty.result(dat))
    dat <- read.dsc.outputs(dat,dsc.outdir,ignore.missing.files)
  dat <- remove.output.suffix(dat)
    
  # POST-FILTER BY CONDITIONS 
  # -------------------------
  # Filter rows of the data frame (or nested list) by each condition.
  # This is second filtering step is necessary to take care of any
  # conditions that couldn't be applied in the pre-filtering step.
  if (!is.null(conditions)) {
    n <- length(conditions)
    for (i in 1:n)
      if (!is.empty.result(dat))
        dat <- filter.by.condition(dat,conditions[i],condition_targets[[i]])
  }

  # ATTEMPT TO FLATTEN RETURN VALUE
  # -------------------------------
  if (is.empty.result(dat)) {
    if (is.list(dat)) {
      dat.new <- matrix(0,0,length(dat))
      colnames(dat.new) <- names(dat)
      dat <- as.data.frame(dat.new,check.names = FALSE)
      rm(dat.new)
    }
  } else {
    dat.new <- flatten.nested.list(dat)
    if (all(!sapply(dat.new,is.list)))
       dat <- as.data.frame(dat.new,check.names = FALSE,
                            stringsAsFactors = FALSE)
    rm(dat.new)
  }

  # REMOVE NON-REQUESTED OUTPUTS
  # ----------------------------
  # Remove any data that were added only to take care of the filtering
  # by condition.
  if (!is.null(conditions))
    dat <- dat[final_outputs]

  # Remove any outputs ending with "output.file", if requested.
  if (omit.filenames) {
    dat <- remove.output.files(dat)
    
  rownames(dat) <- NULL
  return(dat)
}

# Given a condition expression, return (1) the list of targets that
# appear in the condition expression; and (2) the modified condition
# expression in which all instances $(x) are replaced with x.
process.query.condition <- function (condition) {
  pattern <- "(?<=\\$\\().*?(?=\\))"

  # Identify names of all targets, written as $(...), used in
  # condition expression.
  targets <- regmatches(condition,gregexpr(pattern,condition,perl = TRUE))[[1]]
  if (length(targets) == 0)
    stop(paste("Cannot find target syntax $(...) in condition statement:",
               condition))

  # Replace all instances of $(x) in condition expression with x,
  # where x is the name of the target.
  for (x in targets)
    condition <- sub(paste0("\\$\\(",x,"\\)"),x,condition)

  # Return (1) the list of targets that appear in the condition
  # expression, and (2) the modified condition expression in which all
  # instances $(x) are replaced with x.
  return(list(targets = targets,condition = condition))
}

# This is a helper function used in dscquery to build the call to the
# command-line program, "dsc-query".
build.dscquery.call <- function (targets, groups, dsc.outdir, outfile, exec) {
  outfile <- tempfile(fileext = ".csv")
  cmd.str <- sprintf("%s %s -o %s --target \"%s\" --force",exec,dsc.outdir,
                     outfile,paste(targets,collapse = " "))
  if (!is.null(groups))
    cmd.str <- sprintf("%s -g \"%s\"",cmd.str,paste(groups,collapse = " "))
  return(list(outfile = outfile,cmd.str = cmd.str))
}

# For data frame "dat" containing the (raw) output of a dsc-query call,
# filter out rows in which one or more of the targets are unassigned
# or missing. This is a help function used in dscquery.
filter.by.query.targets <- function (dat, targets) {
  col_names <- gsub(":.*|\\.output\\.file","",names(dat))    
  cols      <- which(is.element(col_names,targets))
  rows      <- which(!apply(is.na(dat[cols]),1,any))
  return(dat[rows,])
}

# Filter rows of the data frame (or nested list) "dat" by the given
# expression ("expr") mentioning one or more variables (columns)
# listed in "targets". If one or more targets is unavailable, the
# unmodified data frame is returned.
filter.by.condition <- function (dat, expr, targets) {
  if (all(is.element(targets,names(dat)))) {
    rows <- which(with(dat,eval(parse(text = expr))))
    if (is.data.frame(dat))
      dat <- dat[rows,]
    else
      dat <- lapply(dat,function (x) x[rows])
  }
  return(dat)
}

# This is a helper function used by dscquery that (1) converts the
# "data" data frame to a nested list, and (2) replaces all names of
# files in "module.variable:output" columns with the values of the
# requested targets. Note that the data frame may contain other
# columns containing file names, but only columns specifically with
# names of the form "module.variable:output" are processed here.
#
# This function is more complicated than it might seem from the
# description because it tries to read the targets efficiently by
# reading from each DSC output file no more than once.
read.dsc.outputs <- function (dat, dsc.outdir, ignore.missing.files) {

  # Convert the DSC query result to a nested list.
  dat <- as.list(dat)
  n   <- length(dat)
  for (i in 1:n)
    dat[[i]] <- as.list(dat[[i]])

  # Determine which columns contain names of files that should be
  # read; these are columns of the form "module.variable:output". If
  # there are no such columns, there is nothing to do here. 
  cols <- which(sapply(as.list(names(dat)),is.output.column))
  if (length(cols) == 0)
    return(dat)

  # Create a new nested list data structure in which each element
  # corresponds to a single file containing DSC results; each of these
  # list elements is also a list, in which each of these elements
  # corresponds to a single variable extracted from the DSC results
  # file.
  #
  # Here we need to be careful to skip missing (NA) files.
  files      <- unique(do.call(c,dat[cols]))
  files      <- files[!is.na(files)]
  n          <- length(files)
  out        <- rep(list(list()),n)
  vars       <- rep(as.character(NA),n)
  names(out) <- files
  for (i in cols) {

    # Get the name of the variable to extract.
    x <- names(dat)[i]
    x <- unlist(strsplit(x,"[.]"))[2]
    x <- substr(x,1,nchar(x) - 7)
    vars[i] <- x

    for (j in dat[[i]])
      if (!is.na(j))
        out[[j]][[x]] <- NA
  }

  # Extract the outputs.
  for (i in files) {
    x <- import.dsc.output(i,dsc.outdir,ignore.missing.files)
    if (!is.null(x))
      for (j in names(out[[i]]))
        if (j == "DSC_TIME")
          out[[i]][[j]] <- out$DSC_DEBUG$time$elapsed
        else if (!is.element(j,names(x)))
          stop(sprintf("Variable \"%s\" unavailable in \"%s\"",j,i))
        else
          out[[i]][[j]] <- x[[j]]
  }

  # Copy the DSC outputs from the intermediate nexted list to final
  # nested list, "dat". The names of the DSC output files are replaced
  # by the extracted values of the requested targets.
  for (i in cols) {
    n <- length(dat[[i]])
    v <- vars[i]
    for (j in 1:n) {
      file <- dat[[i]][[j]]
      if (!is.na(file))
        dat[[i]][[j]] <- out[[file]][[v]]
    }
  }
  
  return(dat)
}

# Helper function used by read.dsc.outputs to load the DSC output from
# either an RDS or "pickle" file.
import.dsc.output <- function (file, dsc.outdir, ignore.missing.files) {
  rds <- file.path(dsc.outdir,paste0(file,".rds"))
  pkl <- file.path(dsc.outdir,paste0(file,".pkl"))
  if (file.exists(rds) & file.exists(pkl))
    stop(sprintf(paste("Both %s and %s DSC files exist; DSC output files",
                       "should be cleaned up using \"dsc --clean\""),rds,pkl))
  else if (file.exists(rds))
    out <- read_dsc(rds)
  else if (file.exists(pkl))
    out <- read_dsc(pkl)
  else {
    out <- NULL
    if (!ignore.missing.files)
      stop(sprintf(paste("Unable to read from either %s or %s. You can set",
                         "ignore.missing.files = TRUE to ignore this issue."),
                   rds,pkl))
  }
  return(out)
}

# Given a nested list, x, attempt to "flatten" the elements of x
# whenever it is possible to do so. This is a helper function for
# dscquery.
flatten.nested.list <- function (x) {
  n <- length(x)
  for (i in 1:n)
      
    # If all the list elements are atomic, not NULL, and scalar
    # (i.e., length of 1), then the values can be "flattened" as a vector.
    # If not, then there is nothing to be done.
    if (all(sapply(x[[i]],function (a) !is.null(a) & is.atomic(a) &
                                       length(a) == 1)))
      x[[i]] <- unlist(x[[i]])
  return(x)
}

# Return TRUE if x is a name of a dsc-query output column of the form
# module.variable:output"; otherwise, return FALSE
is.output.column <- function (x) {
  n <- nchar(x)
  if (n < 7)
    return(FALSE)
  else if (length(unlist(strsplit(x,"[:]"))) != 2)
    return(FALSE)
  else
    return(substr(x,n - 6,n) == ":output")
}

# Remove the ":output" suffix from the column names of the data frame
# or names of the list element.
remove.output.suffix <- function (dat) {
  x <- names(dat)
  n <- length(x)
  for (i in 1:n)
    if (is.output.column(x[i])) {
      n    <- nchar(x[i])
      x[i] <- substr(x[i],1,n - 7)
    }
  names(dat) <- x
  return(dat)
}

# Remove all columns or list elements from "dat" with names ending in
# ".output.file".
remove.output.files <- function (dat) {
  i <- which(!sapply(as.list(names(dat)),
                     function (x) {
                       n <- nchar(x)
                       if (n < 12)
                         return(FALSE)
                       else
                         return(substr(x,n - 11,n) == ".output.file")
                       }))
  return(dat[i])
}

# Return TRUE if and only if "dat" is a data frame or list containing
# no results.
is.empty.result <- function (dat)
  any(sapply(dat,length)) == 0

