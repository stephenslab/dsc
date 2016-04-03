score = function(beta_est, true_beta){
  return(list(RMSE=sqrt(mean((beta_est-true_beta)^2))))
}
result = score(beta_est, true_beta)
