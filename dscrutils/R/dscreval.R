#' @title Evaluate R expression and create tuple-like string
#'   encoding of value to be used in DSC.
#'
#' @description Parse and evaluate R expression, and check that the
#'   value is a "simple" atomic object, or a list of simple atomic
#'   objects, then return a string representation of the object.
#'
#' @param x Text of R expression to be parsed by code{parse}.
#'
#' @param envir Environment in which the expression is evaluated. By
#'   default, the expression is evaluated in the global R environment.
#'
#' @param init.env R expression evaluated to initialize the
#'   environment chosen by argument `envir`.
#' 
#' @details The expression must evaluate to a logical, character or
#'   non-complex numeric vector, or a list of vectors of this form. An
#'   error is produced if the expression evaluates to an object that is
#'   not of this form.
#'
#' @return The return value is a string representation the atomic
#'   vector, or list of atomic vectors, in a format similar to nested
#'   tuples in Python.
#'
#' @examples
#'
#' # Generates a string encoding a tuple of numeric values.
#' x1 <- dscreval("c(3,-1,14)")
#' cat(x1,"\n")
#'
#' # Generates a string encoding a tuple of numeric values.
#' x2 <- dscreval("seq(1,2,length.out = 5)")
#' cat(x2,"\n")
#'
#' # Generates a string encoding a tuple of logical values.
#' x3 <- dscreval("1:7 < 5")
#' cat(x3,"\n")
#'
#' # Generates a string encoding a tuple of character values.
#' x4 <- dscreval("c('Illinois','Michigan','Ohio')")
#' cat(x4,"\n")
#'
#' # Generates a string encoding of a nested tuple, in which the first
#' # element contains character values, and the second element contains
#' # numeric values.
#' x5 <- dscreval("list(x = LETTERS[1:5],y = 1:5)")
#' cat(x5,"\n")
#' 
#' \dontrun{
#' 
#' # Produces an error because NULL is not allowed.
#' dscreval("NULL")
#'
#' # Produces an error because complex numbers are not allowed.
#' dscreval("polyroot(c(-1,2,-1,4))")
#'
#' # Produces an error because NULL values are not allowed anywhere in
#' # the data structure.
#' dscreval("vector('list',3)")
#'
#' # Produces an error because lists containing lists are not allowed.
#' dscreval("list(x = LETTERS[1:5],y = 1:5,z = as.list(1:5))")
#' }
#' 
#' @export
#' 
dscreval <- function (x, envir = globalenv(),
                      init.env = quote(set.seed(0))) {

  # Check that the input is a character string.
  if (!is.character(x))
    stop("dscreval input argument \"x\" should be a character string.")

  # Initialize the environment.
  out <- tryCatch(eval(init.env,envir = envir),
                  error = function (e) {
                    msg <- conditionMessage(e)
                    stop(paste("Initialization of environment failed\n",
                               "The error thrown:\n",conditionMessage(e)))
                  })
  
  # Evaluate the R expression in selected environment.
  out <- tryCatch(eval(parse(text = x),envir = envir),
                  error = function (e) {
                    msg <- conditionMessage(e)
                    stop(paste("Evaluation of the following R expression",
                               "failed:\n",x,"\n","The error thrown:\n",
                               conditionMessage(e)))
                  })

  # Verify that the value of the expression is a non-NULL atomic, or a
  # list of non-NULL atomics. (See function "is.simple.atomic" defined
  # below.)
  output.is.valid <- FALSE
  if (!is.simple.atomic(out))
    if (!(is.list(out) & all(sapply(out,is.simple.atomic))))
      stop(paste("Evaluation of the following R expression,\n",x,"\n",
                 "produces a value that is not a simple atomic object,",
                 "nor a list of simple atomic objects."))

  # Convert the value to a string representation of the expression
  # value reminiscent of nested tuples in Python.
  if (is.list(out))
    out <- paste(sapply(out,function (x) paste0("(",atomic2tuple(x),")")),
                 collapse = ",")
  else
    out <- atomic2tuple(out)
  return(out)
}

# Returns TRUE if and only if the input argument is logical, character
# or non-complex numeric.
is.simple.atomic <- function (x)
  is.atomic(x) & !is.complex(x) &
    (is.logical(x) | is.numeric(x) | is.character(x))

# Return a string representation the atomic vector in a format similar
# to tuples in Python. Accepted atomic types are logical, numeric and
# character.
atomic2tuple <- function (x) {
  if (is.logical(x) | is.numeric(x))
    out <- paste(x,collapse = ",")
  else if (is.character(x))
    out <- paste0("\"",paste(x,collapse = "\",\""),"\"")
  else
    stop("Invalid input to atomic2tuple.")
  return(out)
}
