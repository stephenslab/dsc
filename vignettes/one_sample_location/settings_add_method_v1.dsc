normal, t: rnorm.R, rt.R
    seed: R(1:5)
    n: 1000
    true_mean: 0, 1
    $x: x
    $true_mean: true_mean

mean, median, winsor: mean.R, median.R, winsor.R
    x: $x
    @winsor:
      trim: 0.1, 0.2
    $mean: mean

mse: MSE.R
    mean_est: $mean
    true_mean: $true_mean
    $mse: mse

DSC:
    define:
      simulate: normal, t
      estimate: mean, median, winsor
    run: simulate * estimate * mse
    R_libs: psych
    exec_path: R/scenarios, R/methods, R/scores
    output: dsc_result
