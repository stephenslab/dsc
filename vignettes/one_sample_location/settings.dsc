simulate:
    exec: rnorm.R, rt.R
    seed: R(1:5)
    params:
        n: 1000
        true_mean: 0, 1 
    return: x, true_mean

estimate:
    exec: mean.R, median.R
    params:
        x: $x
    return: mean

mse:
    exec: MSE.R
    params:
        mean_est: $mean
        true_mean: $true_mean
    return: mse

DSC:
    run: simulate *
         estimate *
         mse
    exec_path: R/scenarios, R/methods, R/scores
    output: dsc_result
