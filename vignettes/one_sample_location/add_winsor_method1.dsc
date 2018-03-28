#!/usr/bin/env dsc

# This is the same as first_investigation.dsc, except that we add
# another method for estimating the population mean, the "Winsorized
# mean".

# Simulate samples from the normal distribution with mean 0 and
# standard deviation 1.
normal: R(x <- rnorm(n,mean = mu,sd = 1))
  mu: 0
  n: 100
  $data: x
  $true_mean: mu

# Simulate samples from the non-centered t-distribution with 3 degrees
# of freedom.
t: R(x <- mu + rt(n,df = 2))
  mu: 3
  n: 100
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

# Estimate the population mean by computing the Winsorized mean; the
# mean is computed after trimming the top and bottom 10% quantiles.
winsor: R(y <- psych::winsor.mean(x,trim,na.rm = TRUE))
  trim: 0.1
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
    analyze: mean, median, winsor
    score: abs_err, sq_err
  run: simulate * analyze * score
  output: first_investigation
  
