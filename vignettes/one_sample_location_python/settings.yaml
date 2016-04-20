simulate:
    exec: rnorm.py, rt.py
    seed: R(1:10)
    params:
        n: 1000
        true_mean: 0, 1
    return: x, true_mean

estimate:
    exec: mean.py, median.py
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
    exec_path: PY/scenarios, PY/methods, PY/scores
    output: dsc_result