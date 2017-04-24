normal, t:
    exec: rnorm.R, rt.R
    input:
      seed: R(1:5)
      n: 1000
      true_mean: 0, 1
    output:
      $x: x
      $true_mean: true_mean

mean, median:
    exec: mean.R, median.R
    input:
      x: $x
    output:
      $mean: mean

mse:
    exec: MSE.R
    input:
      mean_est: $mean
      true_mean: $true_mean
    output:
      $mse: mse

DSC:
    run: simulate *
         estimate *
         mse
    define: simulate = (normal, t), estimate = (mean, median)
    exec_path: R/scenarios, R/methods, R/scores
    output: dsc_result
