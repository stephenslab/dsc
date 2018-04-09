#!/usr/bin/env dsc

# This DSC file is used for the "DSC Essentials, Part I" tutorial.
#
# SUMMARY
# =======
#
# PIPELINE VARIABLES
# $data       simulated data (vector)
# $true_mean  population mean used to simulate $data (scalar)
# $est_mean   population mean estimate (scalar)
# $error      error in the estimate (scalar)
# 
# MODULE TYPES
# name     inputs                outputs
# ----     ------                -------
# simulate none                  $data, $true_mean
# analyze  $data                 $est_mean
# score    $est_mean, $true_mean $error
#

# Simulate samples from the normal distribution with mean 0 and
# standard deviation 1.
normal: R(x <- rnorm(n,mean = mu,sd = 1))
  nu: 0
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
