score = function(est, truth){
  return(sqrt(mean((est-truth)^2)))
}
result = score(est, truth)
