#' @title R interface for querying DSC output.
#'
#' @description This is an R interface to the \code{dsc-query} program
#' for conveniently extracting and exploring DSC results within the R
#' environment. For additional documentation, run
#' \code{system("dsc-query --help")}.
#'
#' @param dsc.outdir Directory where the DSC output is stored.
#'
#' @param targets Query targets specified as a character string
#' separated by spaces, or by a character vector; for example,
#' \code{targets = "simulate.n analyze score.error"} and \code{targets
#' = c("simulate.n","analyze","score.error")} are equivalent. DSC
#' pipelines (i.e., rows of the returned data frame) in which any of
#' the targets are unassigned or missing (\code{NA}) will be
#' automatically removed from the data frame; to allow for unassigned
#' or missing values in the output columns (or list elements), use
#' argument \code{targets.notreq} instead. This input argument,
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
#' @param conditions Conditions used to filter DSC pipeline
#' results. When \code{conditions = NULL}, no additional filtering of
#' DSC pipelines is performed. Although results can always be filtered
#' \emph{post hoc}, using \code{conditions} to filter can
#' significantly speed up queries when the DSC outputs are very large,
#' as this will filter results, whenever possible, \emph{before} they
#' are loaded into R. Query conditions are specified as R expressions,
#' in which target names are written as \code{$(...)}; for example, to
#' request only results in which the value of parameter \code{sigma}
#' in module \code{simulate} is greater than or equal to \code{0.1},
#' set \code{conditions = "$(simulate.sigma) >= 0.1"} (see below for
#' additional examples). This input argument specifies the
#' \code{--condition} flag in the call to \code{dsc-query}.
#'
#' @param groups Defines module groups. This argument specifies the
#' \code{--groups} flag in the call to \code{dsc-query}. For example,
#' \code{groups = c("method: mean median", "score: abs_err sqrt_err")}
#' will define two module groups, \code{method} and \code{score}.
#'
#' @param omit.file.columns If \code{omit.file.columns = TRUE}, all
#' columns or list elements specifying DSC output files will not be
#' included in the return value (these are list elements or column
#' names ending in "output.file").
#' 
#' @param ignore.missing.file If \code{ignore.missing.file = TRUE},
#' all DSC output files that are missing will have \code{NA} for the
#' file name; when extracting target outputs from files, any outputs
#' with missing files will have their value set to \code{NA}. If
#' \code{ignore.missing.file = FALSE}, \code{dscquery} will throw an
#' error whenever a missing file is encountered.
#'
#' @param exec The command or pathname of the \code{dsc-query}
#' executable.
#'
#' @param return.type If \code{return.type = "data.frame"}, the DSC
#' outputs are returned in a data frame; if \code{return.type =
#' "list"}, the DSC outputs in a list. See "Value" for more
#' information about the different return types, and the benefits (and
#' limitations) of each.
#'
#' @param verbose If \code{verbose = TRUE}, print progress of DSC
#' query command to the console.
#'
#' @return A list or data frame containing the result of the DSC
#' query.
#' 
#' When \code{return.type = "data.frame"}, the output is a
#' data frame.  When possible, DSC outputs are extracted into the
#' columns of the data frame; when this is not possible (e.g., for
#' more complex outputs such as matrices), file names containing the
#' DSC outputs are provided instead. In the latter case, individual
#' outputs can be retrieved using \code{\link{read_dsc}}.
#'
#' When \code{return.type = "list"}, the output is a list, with list
#' elements corresponding to the query targets.
#'
#' A data frame is most convenient with the outputs are not complex.
#'
#' On the other hand, if many outputs are large or complex objects, it
#' may be better to output a list, which is a much more flexible data
#' structure. Note that a list can be later converted to a data frame
#' using \code{\link{as.data.frame}}, or converted to a "tibble" using
#' the \code{\link[tibble]{as_tibble}} function from the tibble
#' package, or converted to many other data structures.
#'
#' When targets are unassigned, these are stored as missing values
#' (\code{NA}) in the appropriate columns.
#'
#' @note We have made considerable effort to prevent column names from
#' being duplicated. However, we have not tested this extensively for
#' possible column name conflicts.
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
#'                 conditions = "$(analyze) == 'mean'")
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
dscquery <- function (dsc.outdir, targets, targets.notreq = NULL,
                      conditions = NULL, groups = NULL, 
                      ignore.missing.file = FALSE,
                      omit.file.columns = FALSE, exec = "dsc-query",
                      return.type = c("data.frame", "list"),
                      verbose = TRUE) {

  browser()
  
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
               "at least one target; they cannot both be \"NULL\""))

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
  
  # Check input argument "ignore.missing.file".
  if (!(is.logical(ignore.missing.file) & length(ignore.missing.file) == 1))
    stop("Argument \"ignore.missing.file\" should be TRUE or FALSE")
  
  # Check input argument "omit.file.columns".
  if (!(is.logical(omit.file.columns) & length(omit.file.columns) == 1))
    stop("Argument \"omit.file.columns\" should be TRUE or FALSE")
  
  # Check input argument "exec".
  if (!(is.character(exec) & length(exec) == 1))
    stop("Argument \"exec\" should be a character vector of length 1")

  # Check and process input argument "return.type".
  return.type <- match.arg(return.type)
  
  # Check input argument "verbose".
  if (!(is.logical(verbose) & length(verbose) == 1))
    stop("Argument \"verbose\" should be TRUE or FALSE")

  split_string = function(value) {
    if (is.character(value) && is.vector(value)) return(strsplit(paste(value, collapse = " "), ' +')[[1]])
    else return(value)
  }

  targets = split_string(targets)
  others = split_string(others)

  # This list keeps track of condition variables
  # It matches `conditions`
  condition_targets = list()
  # This vector keeps track of additional columns involved in `condition` but
  # not in `targets` or `others` and will be removed after use
  additional_columns = vector()
  if (!is.null(conditions)) {
      pattern = '(?<=\\$\\().*?(?=\\))'
      for (i in 1:length(conditions)) {
        condition_targets[[i]] = regmatches(conditions[i], gregexpr(pattern, conditions[i], perl=T))[[1]]
        if (length(condition_targets[[i]]) == 0)
          stop(paste("Cannot find valid target in the format of $(...) in condition statement:", conditions[i]))
        for (item in condition_targets[[i]])
          conditions[i] = sub(paste0('\\$\\(', item, '\\)'), item, conditions[i])
        additional_columns = append(additional_columns, setdiff(condition_targets[i], c(targets, others)))
      }
  }
  if (length(additional_columns))
      others = append(others, additional_columns)

  # RUN DSC QUERY COMMAND
  # ---------------------
  # Generate a temporary directory where the query output will be
  # stored.
  outfile <- tempfile(fileext = ".csv")
  if (is.null(others)) query_target = paste(targets, collapse = " ")
  else query_target = paste(paste(targets, collapse = " "), paste(others, collapse = " "))
  cmd.str <- paste(exec,dsc.outdir,"-o",outfile,"-f",
                   "--target", query_target)
  if (length(groups) >= 1)
    cmd.str <- paste(cmd.str, "-g", paste(paste0('"', groups, '"'), collapse = " "))
  ret = run_cmd(cmd.str, ferr=ifelse(verbose, "", FALSE))

  # LOAD DSC QUERY
  # --------------
  if (verbose)
    cat("Loading dsc-query output from CSV file.\n")
  dat <- read.csv(outfile,header = TRUE,stringsAsFactors = FALSE,
                  check.names = FALSE,comment.char = "",
                  na.strings = "NA")
  n   <- nrow(dat)

  # FILTER BY TARGETS
  # -----------------
  target_cols <- which(gsub(":.*|\\.output\\.file", "", names(dat)) %in% targets)
  # columns indexed by `target_cols` should have at least one non-missing value
  target_rows <- which(apply(dat[, target_cols, drop=FALSE], 1, function(r) !all(r %in% NA)))
  dat <- dat[target_rows, , drop=FALSE]

  # PRE-FILTER BY CONDITIONS
  # ------------------------
  col_names = names(dat)
  query_expr = vector()
  if (length(condition_targets)) {
    for (i in 1:length(condition_targets)) {
      if (sum(condition_targets[[i]] %in% col_names) == length(condition_targets[[i]]))
        query_expr = append(query_expr, conditions[i])
    }
  }
  if (length(query_expr)) {
    query_expr = paste(query_expr, sep = '&')
    dat = subset(dat, eval(parse(text=query_expr)), drop=FALSE)
  }
  
  # PROCESS THE DSC QUERY RESULT
  # ----------------------------
  # Get the indices of all the columns of the form
  # "module.variable:output".

  cols <- which(sapply(as.list(col_names), function (x) {
    n <- nchar(x)
    if (n < 7 | length(unlist(strsplit(x,"[:]"))) != 2)
      return(FALSE)
    else
      return(substr(x,n-6,n) == ":output")
  }))

  dat <- as.list(dat)
  for (i in 1:length(dat)) {
    dat[[i]]        <- data.frame(dat[[i]],stringsAsFactors = FALSE)
    names(dat[[i]]) <- names(dat)[i]
  }

  # Repeat for each column of the form "module.variable:output".
  if (length(cols) > 0) {
    if (verbose)
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
      values <- as.list(rep(NA,n))

      # If any of the serialized data files exist, try to extract the
      # value of the selected output from each of the RDS files.
      # Repeat for each row of the query table.
      dsc.module.files  <- factor(dat[[j]][[1]])
      available.targets <- which(!is.na(dsc.module.files))
      if (length(available.targets) > 0) {

        # Repeat for each target.
        values <- vector("list",length(dsc.module.files))
        for (k in levels(dsc.module.files)) {
          dscfile <- file.path(dsc.outdir,paste0(k,".rds"))
          if (!file.exists(dscfile))
            dscfile <- file.path(dsc.outdir,paste0(k,".pkl"))
          if (!file.exists(dscfile)) {
            dscfile <- NA
            if (!ignore.missing.file) stop(paste("Unable to read", file.path(dsc.outdir,paste0(k,".{rds,pkl}")),
                            "because it does not exist. You can set `ignore.missing.file=TRUE` if you want to skip it."))
          }
          if (is.na(dscfile)) out <- list()
          else out <- read_dsc(dscfile)
          if (var != 'DSC_TIME') {

            # Check that the variable is one of the outputs in the file.
            if (!is.element(var,names(out)))
              stop(paste0("Output \"",var,"\" unavailable in ",dscfile))

            # Extract the value of the variable.
            out <- out[[var]]
          } else
            out <- out$DSC_DEBUG$time$elapsed
          if (is.null(out)) out <- NA
          entries <- which(dsc.module.files == k)
          values[entries] <- rep(list(out),length(entries))
        }
        values <- values[available.targets]
        
        # If all the available values are atomic, not NULL, and scalar
        # (i.e., length of 1), then the values can fit into the column
        # of a data frame. If not, then there is nothing to be done.
        if (all(sapply(values,
                       function (x) !is.null(x) &
                                    is.atomic(x) &
                                    length(x) == 1))) {
          if (verbose)
            cat("extracted atomic values\n")

          dsc.module.files <- vector(class(unlist(values)),
                                     length(dsc.module.files))
          dsc.module.files[] <- NA
          dsc.module.files[available.targets] <- unlist(values)
          dat[[j]] <- data.frame(dsc.module.files,stringsAsFactors = FALSE)
          names(dat[[j]]) <- col.new
          names(dat)[j]   <- col.new
        } else {

          # If (1) all the available values are vectors, (2) the
          # vectors are of the same length, and (3) the vector lengths
          # to not exceed the maximum allowed vector length, then
          # incorporate the vector values into the data frame.
          extract.vectors   <- FALSE
          all.lengths.same <- FALSE
          if (all(sapply(values,function (x) is.vector(x) & !is.list(x))))
            if (length(unique(sapply(values,length)))==1) {
              all.lengths.same <- TRUE
              if (max(sapply(values,length)) <= max.extract.vector)
                extract.vectors <- TRUE
            }
          if (extract.vectors || !atomic_only) {
            if (verbose)
              if (extract.vectors)
                cat("extracted vector values\n")
              else
                cat("extracted complex objects\n")
            if (length(available.targets) < length(dsc.module.files)) {
              tmp = values
              values = list()
              ii = 1
              for (jj in 1:length(dsc.module.files)) {
                if (jj %in% available.targets) {
                  values[[jj]] = tmp[[ii]]
                  ii = ii + 1
                } else {
                  if (extract.vectors)
                    values[[jj]] = rep(NA, length(tmp[[1]]))
                  else
                    values[[jj]] = NA
                }
              }
            }
            if (extract.vectors) {
              dat[[j]] <- data.frame(do.call(rbind,values),
                                   check.names = FALSE,
                                   stringsAsFactors = FALSE)
              names(dat[[j]]) <- paste(col.new,1:ncol(dat[[j]]),sep = ".")
            } else {
              dat[[j]] = values
              names(dat)[j] = col.new
            }
          } else {
            names(dat[[j]]) <- col.new
            if (verbose)
              if (all.lengths.same)
                cat("vectors not extracted (set max.extract.vector =",
                    max(sapply(values,length)),
                    "to extract)\n")
              else
                cat("not extracted (filenames provided)\n")
          }
        }
      }
    }
  }

  dat.names  <- unlist(lapply(dat,names))
  col_names = setdiff(dat.names, col_names)
  if (atomic_only) { 
    # POST-FILTER BY CONDITIONS 
    # -------------------------
    # Remaining columns to filter
    # Output the query result as a data frame.
    dat        <- do.call(cbind,dat)
    names(dat) <- dat.names
    query_expr = vector()
    if (length(condition_targets)) {
      for (i in 1:length(condition_targets)) {
        if (sum(condition_targets[[i]] %in% col_names))
          query_expr = append(query_expr, conditions[i])
      }
    } 
    if (length(query_expr)) {
      query_expr = paste(query_expr, sep = '&')
      dat = subset(dat, eval(parse(text=query_expr)), drop=FALSE)
    }
  
    # REMOVE UNASKED COLUMNS 
    # ----------------------
    if (length(additional_columns)) {
      col_names = setdiff(names(dat), additional_columns)
      dat = dat[, col_names, drop=FALSE]
    }
    if (omit.file.columns) dat <- dat[, !grepl("output.file", dat.names), drop=FALSE]
  } else {
    if (length(col_names) > 0 && any(unlist(condition_targets) %in% col_names))
      cat(paste("Filtering on columns", paste(col_names, collapse = ', '), "are disabled when atomic_only = FALSE is set.\n"))
    cat("A nested list is returned due to option atomic_only = FALSE. To use the result ... (FIXME)\n")
  }
  return(dat)
}

