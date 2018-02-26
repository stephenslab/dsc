normal, t: R(set.seed(seed); x=rnorm(n,mean=true_mean)), R(set.seed(seed); x=true_mean+rt(n,df=2))
    seed: R(1:5)
    n: 1000
    true_mean: 0, 1
    $x: x
    $true_mean: true_mean

mean, median: R(mean = mean($(x))), R(mean = median($(x))) 
    $mean: mean

mse: R(mse = ($(mean)-$(true_mean))^2)
    $mse: mse

DSC:
    define:
      simulate: normal, t
      estimate: mean, median
    run: simulate * estimate * mse
    output: dsc_result