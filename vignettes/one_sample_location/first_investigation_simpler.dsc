#!/usr/bin/env dsc

# This is a slightly simplified version of first_investigation.dsc.

# Simulate samples from the normal distribution with mean 0 and
# standard deviation 1.
normal: R(x <- rnorm(n = 100,mean = 0,sd = 1))
  $data: x
  $true_mean: 0

# Simulate samples from the non-centered t-distribution with 3 degrees
# of freedom.
t: R(x <- 3 + rt(n = 100,df = 2))
  $data: x
  $true_mean: 3

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
    simulate: normal, t
    analyze: mean, median
    score: abs_err, sq_err
  run: simulate * analyze * score
  output: first_investigation
