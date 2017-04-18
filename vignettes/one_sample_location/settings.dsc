normal, t: rnorm.R, rt.R
    seed: R(1:5)
    n: 1000
    true_mean: 0, 1 
    return: x, true_mean

mean, median: mean.R, median.R
    x: $x
    return: mean

mse: MSE.R
    mean_est: $mean
    true_mean: $true_mean
    return: mse

DSC:
    run: simulate *
         estimate *
         mse
    define: simulate = (normal, t), estimate = (mean, median)
    exec_path: R/scenarios, R/methods, R/scores
    output: dsc_result
