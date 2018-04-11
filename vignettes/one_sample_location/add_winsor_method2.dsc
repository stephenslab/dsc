#!/usr/bin/env dsc

# This is the same as add_winsor_method1.dsc, only the "winsor" module
# is implemented more efficiently by inheriting from the "mean" module.

# Simulate samples from the normal distribution with mean 0 and
# standard deviation 1.
normal: normal.R
  mu: 0
  n: 100
  $data: x
  $true_mean: mu

# Simulate samples from the non-centered t-distribution with 2 degrees
# of freedom.
t: t.R
  mu: 3
  n: 100
  $data: x
  $true_mean: mu

# Estimate the population mean by computing the mean value of the
# provided sample.
mean: mean.R
  x: $data
  $est_mean: y

# Estimate the population mean by computing the median value of the
# provided sample.
median: median.R
  x: $data
  $est_mean: y

# Estimate the population mean by computing the Winsorized mean; the
# mean is computed after trimming the top and bottom quantiles.
winsor(mean): winsor.R
  trim: 0.1, 0.2
  
# Compute the error in the estimated mean by taking the squared
# difference between the true mean and the estimated mean.
sq_err: sq.R
  x: $est_mean
  y: $true_mean
  $error: e

# Compute the error in the estimated mean by taking the absolute
# difference between the true mean and the estimated mean.
abs_err: abs.R
  x: $est_mean
  y: $true_mean
  $error: e 
  
DSC:
    define:
      simulate: normal, t
      analyze: mean, median, winsor
      score: abs_err, sq_err
    run: simulate * analyze * score
    exec_path: R
