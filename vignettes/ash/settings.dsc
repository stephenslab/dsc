simulate :
    exec: datamaker.R
    input:
      seed: R(1:5)
      g: Asis(ashr::normalmix(c(2/3,1/3),c(0,0),c(1,2))),
       Asis(ashr::normalmix(rep(1/7,7),c(-1.5,-1,-0.5,0,0.5,1,1.5),rep(0.5,7))),
       Asis(ashr::normalmix(c(1/4,1/4,1/3,1/6),c(-2,-1,0,1),c(2,1.5,1,1)))
      min_pi0: 0
      max_pi0: 1
      nsamp: 1000
      betahatsd: 1
      .alias: args = List()
    output:
      $data: data
      $true_beta : R(data$meta$beta),
      $true_pi0 : R(data$meta$pi0)

shrink:
    exec: runash.R
    input:
      input: $data
      mixcompdist: normal, halfuniform
    output:
      $ash_data: ash_data
      $beta_est:  R(ashr::get_pm(ash_data))
      $pi0_est: R(ashr::get_pi0(ash_data))

score_beta:
    exec: score.R
    input:
      est: $true_beta
      truth: $beta_est
    output:
      $mse_beta: result

score_pi0:
    exec: score.R
    input:
      est: $pi0_est
      truth: $true_pi0
    output:
      $mse_pi: result

DSC:
    run: simulate * shrink * score
    define: score =  (beta_score, pi0_score)
    R_libs: stephens999/ashr (2.0.0+)
    exec_path: bin
    output: dsc_result
