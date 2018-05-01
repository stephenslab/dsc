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
#' @importFrom tools file_path_sans_ext
#' @importFrom yaml yaml.load_file
#' @export
read_dsc <- function(infile) {
  inbase = file_path_sans_ext(infile)
  inext = file_ext(infile)
  if (inext == 'pkl') {
    rt = system(paste('dsc-io', infile, paste0(inbase, '.rds')))
    if (rt != 0)
      stop("DSC data conversion failed (returned a non-zero exit status)")
    return(readRDS(paste0(inbase, '.rds')))
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

#' @export
save_session <- function(start_time, id) {

  time <- as.list(proc.time() - start_time)
  script <- load_script()
  session <- capture.output(print(sessionInfo()))
  return(list(time=time, script=script, replicate=id, session=session))
}
