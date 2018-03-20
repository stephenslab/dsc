#!/usr/bin/env dsc

# This DSC file should match up exactly, or very closely, with the example
# presented in the "Introduction to DSC" tutorial.

# Simulate samples from the standard normal distribution.
normal: R(x = rnorm(n,mean = 0,sd = 1))
  n: 100
  $data: x
  $true_mean: 0

# Simulate samples from the non-centered t-distribution with 3 degrees
# of freedom.
t: R(x = 3 + rt(n,df))
  n: 100
  df: 2
  $data: x
  $true_mean: 3

# Estimate the population mean by computing the mean value of the
# provided sample.
mean: R(y = mean(x))
  x: $data
  $est_mean: y

# Estimate the population mean by computing the median value of the
# provided sample.
median: R(y = median(x))
  x: $data
  $est_mean: y

# Compute the error in the estimated mean by taking the squared
# difference between the true mean and the estimated mean.
sq_err: R(e = ($(est_mean) - $(true_mean))^2)
  $error: e
 
# Compute the error in the estimated mean by taking the absolute
# difference between the true mean and the estimated mean.
abs_err: R(e = abs($(est_mean) - $(true_mean)))
  $error: e 
  
DSC:
    define:
      simulate: normal, t
      analyze: mean, median
      score: abs_err, sq_err
    run: simulate * analyze * score

