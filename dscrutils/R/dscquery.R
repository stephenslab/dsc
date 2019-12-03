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
#' or a module output. Targets that are not assigned are set to
#' \code{NA}. This argument specifies the \code{--target} flag in the
#' \code{dsc-query} call. At least one target must be chosen;
#' \code{targets} cannot be \code{NULL} or empty. These targets will
#' be the names of the columns in the data frame if a data frame is
#' returned, or the names of the list elements if a list is returned.
#' This input argument specifies the \code{--target} option in the
#' \code{dsc-query} call.
#'
#' @param module.output.all Character vector specifying names of
#' modules or module groups in the DSC. For each specified module or
#' module group, an additional list element is provided containing the
#' full module outputs, as well as information recorded by DSC such as
#' the runtime and the replicate number (see the \code{"DSC_DEBUG"}
#' element). This option can be useful for testing or debugging. Note
#' that any module or module group included in
#' \code{module.output.all} must also be included in \code{targets}.
#'
#' @param module.output.files Character vector specifying names of
#' modules or module groups in the DSC. For each specified module or
#' module group, an additional data frame column (or list element)
#' giving the name of the DSC output file is provided. This can be
#' useful if you want to manually load the stored results (e.g., for
#' testing or debugging). For more details on DSC output files, how to
#' interpret the file paths, and how to import the contents of these
#' files into R, see \code{\link{dscread}}. This option can be useful
#' for testing or debugging. Note that any module or module group
#' included in \code{module.output.files} must also be included in
#' \code{targets}.
#' 
#' @param conditions Conditions used to filter DSC pipeline results;
#' rows in which one or more of the conditions evaluate to
#' \code{FALSE} or \code{NA} are removed from the output (removing
#' conditions that evaluate to \code{NA} is convention used by
#' \code{\link{which}}). When \code{conditions = NULL}, no additional
#' filtering of DSC pipelines is performed. Although results can
#' always be filtered \emph{post hoc}, using \code{conditions} to
#' filter can significantly speed up queries when the DSC outputs are
#' very large, as this will filter results, whenever possible,
#' \emph{before} they are loaded into R. Query conditions are
#' specified as R expressions, in which target names are written as
#' \code{$(...)}; for example, to request only results in which the
#' value of parameter \code{sigma} in module \code{simulate} is
#' greater than or equal to \code{0.1}, set \code{conditions =
#' "$(simulate.sigma) >= 0.1"} (see below for additional
#' examples). This input argument specifies the \code{--condition}
#' flag in the call to \code{dsc-query}. All targets used in the
#' conditions must also be included in \code{targets}.
#'
#' @param groups Defines module groups. This argument specifies the
#' \code{--groups} flag in the call to \code{dsc-query}. For example,
#' \code{groups = c("method: mean median", "score: abs_err sqrt_err")}
#' will define two module groups, \code{method} and \code{score}.
#'
#' @param dsc.outfile This optional input argument can be used to
#' provide a previously generated output from the \code{dsc-query}
#' program, in which case it must be the pathname of the output
#' file. This input is mainly intended to be used by developers and
#' expert users for testing or to reproduce previous queries since the
#' \code{dsc-query} output file must exactly agree in the query
#' arguments, otherwise unexpected errors could occur.
#' 
#' @param return.type If \code{return.type = "data.frame"}, the DSC
#' outputs are returned in a data frame; if \code{return.type =
#' "list"}, the DSC output a list. If \code{return.type = "auto"}, a
#' list or data frame is returned depending on which data structure is
#' most appropriate for the DSC outputs. See "Value" for more
#' information about the different return types, and the benefits (and
#' limitations) of each. Note that \code{return.type = "data.frame"}
#' cannot be used when one or more modules or module groups are named
#' in \code{module.output.files}.
#' 
#' @param ignore.missing.files If \code{ignore.missing.files = TRUE},
#' all targets corresponding to DSC output files that cannot be found,
#' or cannot be read (e.g., because they are corrupted), will be
#' treated as if the targets are not assigned (\code{NA}). If
#' \code{ignore.missing.files = FALSE}, \code{dscquery} will generate
#' an error whenever a file cannot be found or read.
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
#' When \code{return.type = "data.frame"}, the output is a data frame.
#' When possible, DSC outputs are extracted into the columns of the
#' data frame; when this is not possible (e.g., for more complex
#' outputs such as matrices), file names containing the DSC outputs
#' are provided instead. A data frame is most convenient with the
#' outputs are not complex.
#'
#' When \code{return.type = "list"}, the output is a list, with list
#' elements corresponding to the query targets. Each top-level list
#' element should have the same length.
#' 
#' When \code{return.type = "auto"}, DSC outputs are extracted into
#' the columns of the data frame unless one or more outputs are large
#' or complex objects, in which case the return value is a list.
#'
#' Note that a list can sometimes be converted to a data frame using
#' \code{\link{as.data.frame}}, or converted to a "tibble" using the
#' \code{\link[tibble]{as_tibble}} function from the tibble package.
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
#' @seealso \code{\link{dscread}}
#' 
#' @examples
#'
#' # Retrieve the number of samples ("simulate.n") and error summary
#' # ("score.error") from all simulations in the "one_sample_location"
#' # DSC experiment.
#' dsc.dir <- system.file("datafiles","one_sample_location",
#'                        "dsc_result",package = "dscrutils")
#' dat1 <- dscquery(dsc.dir,
#'                  targets = c("simulate.n","analyze","score.error"))
#' 
#' # Retrieve the results only for simulations in which the "mean" module
#' # was run. Because this is a condition for a module name, it is
#' # applied before loading the full set of results into R. Therefore,
#' # this type of filtering step can speed up the query when there are
#' # many simulation results.
#' dat2 <- dscquery(dsc.dir,
#'                  targets = c("simulate.n","analyze","score.error"),
#'                  conditions = "$(analyze) == 'mean'")
#'
#' # Return results only for simulations in which the error summary is
#' # greater than 0.2. This condition is applied only after loading the
#' # full set of results into R. Therefore, this type of condition will not
#' # reduce the query runtime.
#' dat3 <- dscquery(dsc.dir,
#'                  targets = c("simulate.n","analyze","score.error"),
#'                  conditions = "$(score.error) > 0.2")
#'
#' # Retrieve the DSC results only for simulations in which the "mean"
#' # module was run, and which which the error summary is greater than
#' # 0.2. The conditions in this case are applied both before and after
#' # loading results into R.
#' dat4 <- dscquery(dsc.dir,
#'                  targets = c("simulate.n","analyze","score.error"),
#'                  conditions = c("$(score.error) > 0.2",
#'                                 "$(analyze) == 'median'"))
#' 
#' # Retrieve some results from the "ash" DSC experiment. In this
#' # example, the beta estimates are vectors, so the results are
#' # extracted into a list by default.
#' dsc.dir2 <- system.file("datafiles","ash","dsc_result",
#'                         package = "dscrutils")
#' dat5 <-
#'   dscquery(dsc.dir2,
#'            targets = c("simulate.nsamp","simulate.g","shrink.mixcompdist",
#'                        "shrink.beta_est","shrink.pi0_est"),
#'            conditions = "$(simulate.g)=='list(c(2/3,1/3),c(0,0),c(1,2))'")
#' 
#' # This is the same as the previous example, but extracts the results
#' # into data frame. Since the vectors cannot be stored in a data frame,
#' # the names of the files storing the vectors are returned instead.
#' dat6 <-
#'   dscquery(dsc.dir2,
#'            targets = c("simulate.nsamp","simulate.g","shrink.mixcompdist",
#'                        "shrink.beta_est","shrink.pi0_est"),
#'            conditions = "$(simulate.g)=='list(c(2/3,1/3),c(0,0),c(1,2))'",
#'            return.type = "data.frame")
#'
#' # See also example("dscread").
#' 
#' @importFrom utils read.csv
#' @importFrom progress progress_bar
#'
#' @export
#'
dscquery <- function (dsc.outdir, targets = NULL, module.output.all = NULL,
                      module.output.files = NULL, conditions = NULL,
                      groups = NULL, dsc.outfile = NULL,
                      return.type = c("auto", "data.frame", "list"),
                      ignore.missing.files = FALSE, exec = "dsc-query",
                      verbose = TRUE) {

  # CHECK & PROCESS INPUTS
  # ----------------------
  # Check input argument "dsc.outdir".
  if (!(is.character(dsc.outdir) & length(dsc.outdir) == 1))
    stop("Argument \"dsc.outdir\" should be a character vector of length 1")
  
  # Check and process input argument "targets".
  if (!(is.character(targets) & is.vector(targets) & length(targets) > 0))
    stop(paste("Argument \"targets\" should be a character vector with",
               "at least one element"))
  all_targets <- c(targets,get.module.names(targets))

  # Check input argument "module.output.all".
  if (!is.null(module.output.all)) {
    if (!(is.character(module.output.all) &
          is.vector(module.output.all) &
          length(module.output.all) > 0))
      stop(paste("Argument \"module.output.all\" should be \"NULL\", or a",
                 "character vector with at least one element"))
    if (length(setdiff(module.output.all,targets)) > 0)
      stop(paste("All modules and module groups included in",
                 "\"module.output.all\" must also be included in",
                 "\"targets\""))
  }

  # Check input argument "module.output.files".
  if (!is.null(module.output.files)) {
    if (!(is.character(module.output.files) & is.vector(module.output.files) &
          length(module.output.files) > 0))
      stop(paste("Argument \"module.output.files\" should be \"NULL\", or a",
                 "character vector with at least one element"))
    if (length(setdiff(module.output.files,targets)) > 0)
      stop(paste("All modules and module groups included in",
                 "\"module.output.files\" must also be included in",
                 "\"targets\""))
  }
  
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
  
  # Check input argument "dsc.outfile".
  if (!is.null(dsc.outfile))
    if (!(is.character(dsc.outfile) & length(dsc.outfile) == 1))
      stop(paste("Argument \"dsc.outfile\" should either be \"NULL\" or a",
                 "character vector of length 1"))
  
  # Check and process input argument "return.type".
  return.type <- match.arg(return.type)
  if (return.type == "data.frame" & length(module.output.all) > 1)
    stop(paste("Complete module outputs requested with \"module.output.all\"",
               "cannot be returned in a data frame; select return.type =",
               "\"list\" or return.type = \"auto\" instead"))

  # Check input argument "ignore.missing.files".
  if (!(is.logical(ignore.missing.files) & length(ignore.missing.files) == 1))
    stop("Argument \"ignore.missing.files\" should be TRUE or FALSE")
  
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
  #
  # Here is where we also check that each of the targets mentioned in
  # the condition expressions is also included in the "targets" argument.
  if (!is.null(conditions)) {
    n                 <- length(conditions)
    condition_targets <- vector("list",n)
    for (i in 1:n) {
      out <- process.query.condition(conditions[i])
      condition_targets[[i]] <- out$targets
      conditions[i]          <- out$condition
      if (!all(is.element(condition_targets[[i]],all_targets)))
        stop(paste("All targets mentioned in conditions must also be",
                   "mentioned in \"targets\""))
    }
  }

  # RUN DSC QUERY COMMAND
  # ---------------------
  # Now we are ready to run dsc-query with all targets. Note that
  # although the dsc-query program has the option to pass in
  # conditions, this feature is not used here, as the queries in this
  # interface are specified as R expressions.
  if (is.null(dsc.outfile)) {
    out         <- build.dscquery.call(targets,groups,dsc.outdir,exec)
    dsc.outfile <- out$outfile
    cmd.str     <- paste(out$cmd.str, '-o', dsc.outfile)
    if (verbose)
      cat(paste("Calling:", out$cmd.str), '\n')
    run_cmd(cmd.str)
  }
  
  # IMPORT DSC QUERY RESULTS
  # ------------------------
  # As a safeguard, we check for any duplicated column (or list
  # element) names, and if there are any, we halt and report an error.
  dat <- read.csv(dsc.outfile,header = TRUE,stringsAsFactors = FALSE,
                  check.names = FALSE,comment.char = "",
                  na.strings = "NA")
  if (any(duplicated(names(dat))))
    stop("One or more names in dsc-query output are the same")
  
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
  if (verbose)
    cat(sprintf("Loaded dscquery output table with %d rows and %d columns.\n",
                nrow(dat),ncol(dat)))

  # EXTRACT DSC OUTPUT
  # ------------------
  # Extract outputs from DSC files for all columns with names of the
  # form "module.variable:output". After this step, "dat" will become a
  # nested list, in which each element dat[[i]][j]] is the value of
  # target i in pipeline j.
  dat.unextracted <- dat
  if (!is.empty.result(dat))
    dat <- read.dsc.outputs(dat,dsc.outdir,ignore.missing.files,verbose)
  dat <- remove.output.suffix(dat)

  # EXTRACT FULL MODULE OUTPUTS
  # ---------------------------
  n <- length(module.output.all)
  if (n > 0) {
    full.outputs        <- vector("list",n)
    names(full.outputs) <- module.output.all
    if (verbose)
      pb <- progress_bar$new(format = paste("- Loading module outputs [:bar]",
                                            ":percent eta: :eta"),
                             total = n,clear = FALSE,width = 60,
                             show_after = 0,force = TRUE)
    else
      pb <- null_progress_bar$new()
    pb$tick(0)

    # Extract the full module outputs for each selected module or
    # module group.
    for (i in module.output.all) {
      pb$tick()
      j <- paste(i,"output.file",sep = ".")
      if (!is.element(j,names(dat)))
        stop(paste("One or more entries in \"module.output.all\" do not",
                   "specify a valid module or module group"))
      x <- dat[[j]]
      m <- length(x)
      if (m > 0)
        for (j in 1:m)
          x[j] <-
            list(import.dsc.output(x[[j]],dsc.outdir,ignore.missing.files))
      full.outputs[[i]] <- x
    }

    # Combine everything into a single list or data frame, and
    # re-order the columns (or list elements).
    names(full.outputs) <- paste(names(full.outputs),"output.all",sep = ".")
    cols <- names(dat)
    for (i in module.output.all) {
      j    <- which(cols == paste(i,"output.file",sep = "."))
      m    <- length(cols)
      cols <- c(cols[1:j],paste(i,"output.all",sep = "."),cols[(j+1):m])
    }
    dat <- c(dat,full.outputs)
    dat <- dat[cols]
    rm(full.outputs)
  }

  # OPTIONALLY FLATTEN RETURN VALUE
  # -------------------------------
  # Handle the edge case when there are no results to return (i.e.,
  # zero rows in data frame).
  if (is.empty.result(dat)) {
    if (return.type == "data.frame" || return.type == "auto") {
      dat.new           <- matrix(0,0,length(dat))
      colnames(dat.new) <- names(dat)
      dat               <- as.data.frame(dat.new,check.names = FALSE)
      rm(dat.new)
    }
  } else if (return.type == "data.frame") {

    # Flatten all the results into a data frame. For anything that
    # cannot be inserted into a column of a data frame, give the
    # output file instead.
    dat <- flatten.nested.list(dat)
    n   <- length(dat)
    some.cols.unextracted <- FALSE
    for (i in 1:n)
      if (is.list(dat[[i]])) {
        dat[[i]] <- dat.unextracted[[paste0(names(dat)[i],":output")]]
        if (!some.cols.unextracted) {
          some.cols.unextracted <- TRUE
          message(paste("Results for one or more targets were not added to",
                        "the data frame because their contents are complex;",
                        "consider setting return.type = \"list\" to retrieve",
                        "the results for these targets"))
        }
      }
    dat <- as.data.frame(dat,check.names = FALSE,stringsAsFactors = FALSE)
  } else if (return.type == "auto") {
      
    # If all the outputs can be stored in a data frame, do so.
    dat <- flatten.nested.list(dat)
    if (all(!sapply(dat,is.list)))
      dat <- as.data.frame(dat,check.names = FALSE,stringsAsFactors = FALSE)
    else
      message(paste("dscquery is returning a list because one or more",
                    "outputs are complex; consider converting the list to",
                    "a tibble using the \"tibble\" package"))
  } else if (return.type  == "list") {
    dat <- flatten.nested.list(dat)
    if (all(!sapply(dat,is.list)))
      message(paste("return.type = \"list\" was chosen, but results can also",
                    "be returned as a data frame with return.type =",
                    "\"data.frame\" or return.type = \"auto\"; a data frame",
                    "may be more convenient for analyzing these results"))
  }
  rm(dat.unextracted)
  
  # POST-FILTER BY CONDITIONS 
  # -------------------------
  # Filter rows of the data frame (or list) by each condition.
  # This is second filtering step is necessary to take care of any
  # conditions that couldn't be applied in the pre-filtering step.
  if (!is.null(conditions)) {
    n <- length(conditions)
    for (i in 1:n)
      if (!is.empty.result(dat))
        dat <- filter.by.condition(dat,conditions[i],condition_targets[[i]])
  }

  # REMOVE NON-REQUESTED OUTPUTS
  # ----------------------------
  # Remove any outputs that were not requested by the user, and order
  # the outputs so that they are in the same order as the user
  # specified them in the call to dscquery.
  cols <- c("DSC",
            targets,
            paste(module.output.files,"output.file",sep = "."),
            paste(module.output.all,"output.all",sep = "."))
  cols <- cols[is.element(cols,names(dat))]
  dat  <- dat[cols]
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
build.dscquery.call <- function (targets, groups, dsc.outdir, exec) {
  outfile <- tempfile(fileext = ".csv")
  cmd.str <- sprintf("%s %s -o %s --target \"%s\" --force",exec,dsc.outdir,
                     outfile,paste(targets,collapse = " "))
  if (!is.null(groups))
    cmd.str <- sprintf("%s -g %s",cmd.str,paste(paste0('"', groups, '"'),
                                                collapse = " "))
  return(list(outfile = outfile,cmd.str = cmd.str))
}

# Filter rows of the data frame (or nested list) "dat" by the given
# expression ("expr") mentioning one or more variables (columns)
# listed in "targets". If one or more targets is unavailable, the
# unmodified data frame is returned.
filter.by.condition <- function (dat, expr, targets) {
  if (all(is.element(targets,names(dat)))) {
    tryCatch({
      rows <- which(with(dat,eval(parse(text = expr))))
    }, error = function (e) stop(paste("Unable to evaluate expression:",expr)))
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
read.dsc.outputs <- function (dat, dsc.outdir, ignore.missing.files, verbose) {

  # Convert the DSC query result to a nested list. Here we use a
  # "trick", setting all missing values to NA with logical type. This
  # helps later on when using "unlist" to combine several values that
  # are of different types, some of which are NA; with NAs set to
  # logical, "unlist" should do a better job getting the best type.
  #
  # Note that the "as.logical" part is redundant, this is helpful to
  # make it explicit that the NA is of type logical.
  dat <- as.list(dat)
  n   <- length(dat)
  for (i in 1:n) {
    x           <- as.list(dat[[i]])
    x[is.na(x)] <- as.logical(NA)
    dat[[i]]    <- x
  }
  
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
  #
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
  if (verbose)
    pb <-
      progress_bar$new(format = "- Loading targets [:bar] :percent eta: :eta",
                       total = n,clear = FALSE,width = 60,show_after = 0)
  else
    pb <- null_progress_bar$new()
  pb$tick(0)
  for (i in files) {
    pb$tick()
    x <- import.dsc.output(i,dsc.outdir,ignore.missing.files)
    if (!is.null(x))
      for (j in names(out[[i]]))
        if (j == "DSC_TIME")
          out[[i]][[j]] <- x$DSC_DEBUG$time$elapsed
        else if (!is.element(j,names(x)))
          stop(sprintf("Variable \"%s\" unavailable in \"%s\"",j,i))
        else
          out[[i]][j] <- list(x[[j]])
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
        dat[[i]][j] <- list(out[[file]][[v]])
    }
  }
  
  return(dat)
}

# Helper function used by read.dsc.outputs to load the DSC output from
# either an RDS or "pickle" file.
import.dsc.output <- function (outfile, outdir, ignore.missing.files) {
  out <- dscread(outdir,outfile)
  if (is.null(out) & !ignore.missing.files)
    stop(sprintf(paste("Unable to read from DSC output file %s. You can set",
                       "ignore.missing.files = TRUE to ignore this issue."),
                 outfile))
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

# Extract the module names or module group names from a set of targets.
get.module.names <- function (targets) {
  n   <- length(targets)
  out <- NULL
  for (i in 1:n) {
    x <- unlist(strsplit(targets[i],"[.]"))
    if (length(x) > 1)
      out <- c(out,x[1])
  }
  return(out)
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

# Return TRUE if and only if "dat" is a data frame or list containing
# no results.
is.empty.result <- function (dat)
  any(sapply(dat,length)) == 0
