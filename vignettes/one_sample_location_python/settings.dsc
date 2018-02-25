rnorm, rt: rnorm.py, rt.py
    seed: R(1:10)
    n: 1000
    true_mean: 0, 1
    $x: x
    $true_mean: true_mean

mean, median: mean.py, median.py
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
    exec_path: PY/scenarios, PY/methods, PY/scores
    output: dsc_result