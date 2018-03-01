# TO DO: Briefly describe what this function does. We do not need
# roxygen2 documentation for this function since it will not be
# exported.
#
# NOTES:
# 
#   - Evaluates the expression in the base R environment.
# 
dscreval <- function (x) {

  # Evaluate the R expression in environment of package:base.
  out <- tryCatch(eval(parse(text = x),envir = baseenv()),
                  error = function (e) {
                    msg <- conditionMessage(e)
                    stop(paste("Evaluation of the following R expression",
                               "failed:\n",x,"\n","The error thrown:\n",
                               conditionMessage(e)))
                  })

  return(out)
}
