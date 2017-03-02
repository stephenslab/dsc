simulate:
    exec: datamaker.R
    .alias: An, Bn, Cn
    seed: R(1:5)
    params:
        min_pi0: 0
        max_pi0: 1
        nsamp: 1000
        betahatsd: 1
        exec[1]:
          g: Asis(ashr::normalmix(c(2/3,1/3),c(0,0),c(1,2))),
        exec[2]:
          g: Asis(ashr::normalmix(rep(1/7,7),c(-1.5,-1,-0.5,0,0.5,1,1.5),rep(0.5,7))),
        exec[3]:
          g: Asis(ashr::normalmix(c(1/4,1/4,1/3,1/6),c(-2,-1,0,1),c(2,1.5,1,1)))
        .alias: args = List()
    return: data, true_beta = R(data$meta$beta), true_pi0 = R(data$meta$pi0)

shrink:
    exec: runash.R
    .alias: ash_n, ash_hu
    params:
        input: $data
        exec[1]:
          mixcompdist: normal
        exec[2]:
          mixcompdist: halfuniform
    return: ash_data, beta_est = R(ashr::get_pm(ash_data)),
            pi0_est = R(ashr::get_pi0(ash_data))

beta_score:
    exec: score.R
    .alias: score_beta
    params:
        beta_true: $true_beta
        beta_est: $beta_est
        .alias: est = beta_est, truth = beta_true
    return: result

pi0_score(beta_score):
    .alias: score_pi0
    params:
        pi0_est: $pi0_est
        pi0: $true_pi0
        .alias: est = pi0_est, truth = pi0

DSC:
    run:
      An_ash_n: simulate[1] * shrink[1] * (beta_score, pi0_score)
      An_ash_hu: simulate[1] * shrink[2] * (beta_score, pi0_score)
      Bn_ash_n: simulate[2] * shrink[1] * (beta_score, pi0_score)
      Bn_ash_hu: simulate[2] * shrink[2] * (beta_score, pi0_score)
      Cn_ash_n: simulate[3] * shrink[1] * (beta_score, pi0_score)
      Cn_ash_hu: simulate[3] * shrink[2] * (beta_score, pi0_score)
    R_libs: stephens999/ashr (2.0.0+)
    exec_path: bin
    output: dsc_result