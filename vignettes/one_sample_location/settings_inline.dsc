#!/usr/bin/env dsc

normal: R(x = rnorm(n,0,1))
  n: 100
  $data: x
  $true_mean: 0

t: R(x = 3+rt(n,df))
  n: 100
  df: 2
  $data: x
  $true_mean: 3

mean: R(y = mean($(data))) 
  $est_mean: y

median: R(y = median($(data)))
  $est_mean: y

sq_err: R(e = ($(est_mean) - $(true_mean))^2)
  $error: e
 
abs_err: R(e = abs($(est_mean) - $(true_mean)))
  $error: e 
  
DSC:
    define:
      simulate: normal, t
      analyze: mean, median
      score: abs_err, sq_err
    run: simulate * analyze * score
    output: dsc_result
