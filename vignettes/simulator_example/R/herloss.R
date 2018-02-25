metric = function(model, out) {
  return(abs(model$mu - out$fit))
}
