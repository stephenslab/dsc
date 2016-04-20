simulate:
    exec: rnorm.py, rt.R
    seed: R(1:10)
    params:
        n: 1000
        true_mean: 0, 1
    return: x, true_mean

estimate:
    exec: mean.R, median.py
    params:
        x: $x
    return: mean

mse:
    exec: MSE.py
    params:
        mean_est: $mean
        true_mean: $true_mean
    return: mse

DSC:
    run: simulate *
         estimate *
         mse
    R_libs: psych
    exec_path: PY/scenarios, PY/methods, PY/scores,
               R/scenarios, R/methods
    output: dsc_result