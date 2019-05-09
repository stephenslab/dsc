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
run_cmd <- function(cmd_str, shell_exec="/bin/bash", fout='',ferr='') {
  if (ferr!=FALSE) {
    write("Running shell command:", stderr())
    write(cmd_str, stderr())
  }
  out <- system2(shell_exec, args = c("-c", shQuote(cmd_str)), stdout = fout, stderr=ferr)
  if (out != 0 && fout != TRUE && ferr != TRUE)
    stop(paste(strsplit(cmd_str, " +")[[1]][1], "command failed (returned a non-zero exit status)"))
  return(out)
}

# finds all .R and .r files within a folder and sources them
source_dir <- function(folder, recursive = TRUE, ...) 
{ 
    files <- list.files(folder, pattern = "[.][rR]$", 
                        full.names = TRUE, recursive = recursive)
    if (length(files))
      src <- invisible(lapply(files, source, ...))
}

source_dirs <- function(folders, ...) {
  for (folder in folders) source_dir(folder, ...)
}
