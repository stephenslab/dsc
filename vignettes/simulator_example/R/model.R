set.seed(seed)
simulate = function(n, prob, mu=2) {
  contam <- runif(n) < prob
  x <- rep(NA, n)
  x[contam] <- rexp(sum(contam))
  x[!contam] <- rnorm(sum(!contam))
  x <- mu + x # true mean is mu
  return(list(x=x, mu=mu))
}
