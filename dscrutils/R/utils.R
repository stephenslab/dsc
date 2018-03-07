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
read_dsc <- function(infile) {
  inbase = file_path_sans_ext(infile)
  inext = file_ext(infile)
  rt = 0
  if (inext == 'pkl')
    rt = system(paste('dsc-io', infile, paste0(inbase, '.rds')))
  if (rt != 0)
    stop("DSC data conversion failed (returned a non-zero exit status)")
  return(readRDS(paste0(inbase, '.rds')))
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
    return(normalizePath(sys.frames()[[1]]$ofile))
  }
}

load_script <- function() {
  fileName <- thisFile()
  return(readChar(fileName, file.info(fileName)$size))
}
