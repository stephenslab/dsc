#!/usr/bin/env dsc

# This is the same as first_investigation_simpler.dsc, except that we
# introduce a "seed" parameter in the "normal" module to generate
# multiple normally-distributed data sets with different sequences of
# pseudorandom numbers.
normal: R(set.seed(seed); x <- rnorm(n,mean = mu,sd = 1))
  seed: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  n: 100
  mu: 0
  $data: x
  $true_mean: mu

# Estimate the population mean by computing the mean value of the
# provided sample.
mean: R(y <- mean(x))
  x: $data
  $est_mean: y

# Estimate the population mean by computing the median value of the
# provided sample.
median: R(y <- median(x))
  x: $data
  $est_mean: y

# Compute the error in the estimated mean by taking the squared
# difference between the true mean and the estimated mean.
sq_err: R(e <- (x - y)^2)
  x: $est_mean
  y: $true_mean
  $error: e

# Compute the error in the estimated mean by taking the absolute
# difference between the true mean and the estimated mean.
abs_err: R(e <- abs(x - y))
  x: $est_mean
  y: $true_mean
  $error: e

DSC:
  define:
    analyze: mean, median
    score: abs_err, sq_err
  run: normal * analyze * score

