#!/usr/bin/env dsc

normal: normal.R
  n: 100
  $data: x
  $true_mean: 0

t: t.R
  n: 100
  df: 2
  $data: x
  $true_mean: 3

mean: mean.R
  x: $data
  $est_mean: y

median: median.R
  x: $data
  $est_mean: y

sq_err: sq.R
  a: $est_mean
  b: $true_mean
  $error: e
 
abs_err: abs.R
  a: $est_mean
  b: $true_mean
  $error: e 
  
DSC:
    define:
      simulate: normal, t
      analyze: mean, median
      score: abs_err, sq_err
    run: simulate * analyze * score
    exec_path: R
    output: dsc_result
