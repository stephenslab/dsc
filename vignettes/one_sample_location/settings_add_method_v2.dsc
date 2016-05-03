simulate:
    exec: rnorm.R, rt.R
    seed: R(1:10)
    params:
        n: 1000
        true_mean: 0, 1
    return: x, true_mean

estimate:
    exec: mean.R, median.R
    params:
        x: $x
    return: mean

estimate_winsor:
    exec: winsor.R
    params:
        x: $x
        trim: 0.1, 0.2
    return: mean

mse:
    exec: MSE.R
    params:
        mean_est: $mean
        true_mean: $true_mean
    return: mse

DSC:
    run: simulate *
         (estimate, estimate_winsor) *
         mse
    R_libs: psych
    exec_path: R/scenarios, R/methods, R/scores
    output: dsc_result