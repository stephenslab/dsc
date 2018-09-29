merge_lists <- function(x, y, ...)
{
  if(length(x) == 0)
    return(y)
  if(length(y) == 0)
    return(x)
  for (i in 1:length(names(y)))
    x[names(y)[i]] = y[i]
  return(x)
}

#' @importFrom tools file_ext
#' @importFrom yaml yaml.load_file
#'
#' @export
#'
read_dsc <- function(infile) {
  inext = file_ext(infile)
  if (inext == 'pkl') {
    ## FIXME: not an efficient method
    outfile = tempfile(fileext = '.rds')
    run_cmd(paste('dsc-io', infile, outfile, '-f'), verbose=FALSE)
    if (!file.exists(outfile))
      stop(paste("DSC data conversion failed for", infile))
    return(readRDS(outfile))
  } else if (inext == 'yml') {
    return(yaml.load_file(infile))
  } else {
    return(readRDS(infile))
  }
}

#' @export
load_inputs <- function(files, loader) {
  if (length(files) == 1) {
    return(loader(files[1]))
  }
  out <- list()
  for (i in 1:length(files)) {
    out <- merge_lists(out, loader(files[i]))
  }
  return(out)
}


thisFile <- function() {
  cmdArgs <- commandArgs(trailingOnly = FALSE)
  needle <- "--file="
  match <- grep(needle, cmdArgs)
  if (length(match) > 0) {
    ## Rscript
    path <- cmdArgs[match]
    path <- gsub("\\~\\+\\~", " ", path)
    return(normalizePath(sub(needle, "", path)))
  } else {
    ## 'source'd via R console
    return(sys.frames()[[1]]$ofile)
  }
}

load_script <- function() {
  fileName <- thisFile()
  return(ifelse(!is.null(fileName), readChar(fileName, file.info(fileName)$size), ""))
}

#' @importFrom utils capture.output
#' @importFrom utils sessionInfo
#' @export
save_session <- function(start_time, id) {

  time <- as.list(proc.time() - start_time)
  script <- load_script()
  session <- capture.output(print(sessionInfo()))
  return(list(time=time, script=script, replicate=id, session=session))
}

#' @export
run_cmd <- function(cmd_str, verbose=TRUE) {
  if (verbose) {
    write("Running shell command:", stderr())
    write(cmd_str, stderr())
  }
  out <- system(cmd_str,ignore.stdout = !verbose,ignore.stderr = !verbose)
  if (out != 0)
    stop(paste(strsplit(cmd_str, " +")[[1]][1], "command failed (returned a non-zero exit status)"))
  return(out)
}
