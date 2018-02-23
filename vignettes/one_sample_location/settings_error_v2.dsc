normal, t: rnorm.R, rt.R
    seed: R(1:5)
    n: 1000
    true_mean: 0, 1
    $x: x
    $true_mean: true_mean

mean, median: meow.R, median.R
    x: $x
    $mean: mean

mse: MSE.R
    mean_est: $mean
    true_mean: $true_mean
    $mse: mse

DSC:
    define:
      simulate: normal, t
      estimate: mean, median
    run: simulate * estimate * mse
    exec_path: R/scenarios, R/methods, R/scores
    output: dsc_result
