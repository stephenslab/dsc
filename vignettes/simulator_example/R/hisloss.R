metric = function(model, out) {
  return((model$mu - out$fit)^2)
}
