simulate: model.R + R(m = simulate(n, mu, prob))
  seed: R(1:10)
  n: 50
  prob: R(seq(0,1,length=6))
  $model: m

my_method, their_method: (my.R, their.R) + R(fit = method($(model)$x)$fit)
  $fit: fit

abs, mse: (herloss.R, hisloss.R) + R(score = metric($(model), $(fit)))
  $score: score

DSC:
  define:
    method: my_method, their_method
    score: abs, mse
  run: simulate * method * score
  output: simulator_example
  exec_path: R
