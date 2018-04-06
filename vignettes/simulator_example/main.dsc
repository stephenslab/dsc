simulate: model.R + R(m = simulate(n, prob))
  n: 50
  prob: R{seq(0,1,length=6)}
  $model: m

my_method, their_method: (my.R, their.R) + R(fit = method($(model)$x)$fit)
  $fit: fit

abs, mse: (herloss.R, hisloss.R) + R(score = metric($(model)$mu, $(fit)))
  $score: score

DSC:
  define:
    method: my_method, their_method
    score: abs, mse
  run: simulate * method * score
  replicate: 10
  output: simulator_results
  exec_path: R
