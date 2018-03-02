# TO DO: Briefly describe what this function does. We do not need
# roxygen2 documentation for this function since it will not be
# exported.
#
# NOTES:
# 
#   - Evaluates the expression in the base R environment.
#

#' @title Parse and evaluate R expression, and check that the value is
#' a "simple" atomic object, or a list of simple atomic objects.
#'
#' @examples
#'
#' # Generates a numeric vector.
#' dscreval("c(3,-1,14)")
#'
#' # Generates a numeric vector.
#' dscreval("seq(1,2,length.out = 5)")
#'
#' # Generates a logical vector.
#' dscreval("1:10 < 5")
#'
#' # Generates a list with two vectors.
#' dscreval("list(x = LETTERS[1:5],y = 1:5)")
#' 
#' # Generates an error.
#' dscreval("NULL")
#'
#' # Generates an error.
#' dscreval("polyroot(c(-1,2,-1,4))")
#'
#' # Generates an error.
#' dscreval("vector('list',3)")
#'
#' # Generates an error.
#' dscreval("list(x = LETTERS[1:5],y = 1:5,z = as.list(1:5))")
dscreval <- function (x) {

  # Check that the input is a character string.
  if (!is.character(x))
    stop("dscreval input argument \"x\" should be a character string.")
    
  # Evaluate the R expression in environment of package:base.
  out <- tryCatch(eval(parse(text = x),envir = baseenv()),
                  error = function (e) {
                    msg <- conditionMessage(e)
                    stop(paste("Evaluation of the following R expression",
                               "failed:\n",x,"\n","The error thrown:\n",
                               conditionMessage(e)))
                  })

  # Check that the value of the expression is a non-NULL atomic
  if (is.simple.atomic(out))
    return(out)
  else if (is.list(out) & all(sapply(out,is.simple.atomic)))
    return(out)
  else
    stop(paste("Evaluation of the following R expression,\n",x,"\n",
               "produces a value that is not a simple atomic object,",
               "nor a list of simple atomic objects."))
  return(out)
}

# Returns TRUE if and only if the input argument is atomic, and not
# complex, "raw" or NULL.
is.simple.atomic <- function (x)
  is.atomic(x) & !(is.complex(x) | is.raw(x) | is.null(x) )
