## @knitr models

make_my_model <- function(n, prob) {
  new_model(name = "contaminated-normal",
            label = sprintf("Contaminated normal (n = %s, prob = %s)", n, prob),
            params = list(n = n, mu = 2, prob = prob),
            simulate = function(n, mu, prob, nsim) {
              # this function must return a list of length nsim
              contam <- runif(n * nsim) < prob
              x <- matrix(rep(NA, n * nsim), n, nsim)
              x[contam] <- rexp(sum(contam))
              x[!contam] <- rnorm(sum(!contam))
              x <- mu + x # true mean is mu
              return(split(x, col(x))) # make each col its own list element
            })
}
