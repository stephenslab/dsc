# module alias and executables
simulate: datamaker.R
    # module input
    seed: R(1:5)
    g: raw(ashr::normalmix(c(2/3,1/3),c(0,0),c(1,2))),
       raw(ashr::normalmix(rep(1/7,7),c(-1.5,-1,-0.5,0,0.5,1,1.5),rep(0.5,7))),
       raw(ashr::normalmix(c(1/4,1/4,1/3,1/6),c(-2,-1,0,1),c(2,1.5,1,1)))
    min_pi0: 0
    max_pi0: 1
    nsamp: 1000
    betahatsd: 1
    # module output
    $data: data
    $true_beta: raw(data$meta$beta)
    $true_pi0: raw(data$meta$pi0)
    # global decoration
    @alias: args = list()
    @options: queue = midway

shrink: runash.R
    # module input
    input: $data
    mixcompdist: normal, halfuniform
    # module output
    $ash_data: ash_data
    $beta_est: raw(ashr::get_pm(ash_data))
    $pi0_est: raw(ashr::get_pi0(ash_data))

score_beta: score.R
    # module input
    est: $true_beta
    truth: $beta_est
    # module output
    $mse_beta: result

score_pi0: score.R
    # module input
    est: $pi0_est
    truth: $true_pi0
    # module output
    $mse_pi: result

DSC:
    define: score = (beta_score, pi0_score)
    run: simulate * shrink * score
    R_libs: stephens999/ashr (2.0.0+)
    exec_path: bin
    output: dsc_result
