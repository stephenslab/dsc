rt, rcauchy: rt.R, rcauchy.R
    seed: R(1:5)
    n: 1000
    true_loc: 0, 1
    $x: x
    $true_loc: true_loc

winsor1, winsor2: winsor1.R, winsor2.R
    x: $x
    @winsor1:
      fraction: 0.05
    @winsor2:
      multiple: 3
    $x: x

mean, median: mean.R, median.R
    x: $x
    $loc: loc

mse: MSE.R
    mean_est: $loc
    true_mean: $true_loc
    $mse: mse

DSC:
    define:
      simulate: rt, rcauchy
      transform: winsor1, winsor2
      estimate: mean, median
    run: simulate *
         (transform * estimate, estimate) *
         mse
    exec_path: R
    output: dsc_result