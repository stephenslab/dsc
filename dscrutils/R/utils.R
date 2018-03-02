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

read_dsc <- function(infile)
{
  library(tools)
  inbase = file_path_sans_ext(infile)
  inext = file_ext(infile)
  rt = 0
  if (inext == 'pkl')
    rt = system(paste('dsc-io', infile, paste0(inbase, '.rds')))
  if (rt != 0)
    stop("DSC data conversion failed (returned a non-zero exit status)")
  return(readRDS(paste0(inbase, '.rds')))
}
