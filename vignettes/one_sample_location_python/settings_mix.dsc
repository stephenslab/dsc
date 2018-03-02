#!/usr/bin/env dsc
rnorm, rt: rnorm.py, rt.R
    seed: R(1:10)
    n: 1000
    true_mean: 0, 1
    $x: x
    $true_mean: true_mean

mean, median: mean.R, median.py
    x: $x
    $mean: mean

mse: MSE.py
    mean_est: $mean
    true_mean: $true_mean
    $mse: mse

DSC:
    define:
      simulate: rnorm, rt
      estimate: mean, median
    run: simulate * estimate * mse
    R_libs: psych
    exec_path: PY/scenarios, PY/methods, PY/scores,
               R/scenarios, R/methods
    output: dsc_result
