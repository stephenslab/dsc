#!/usr/bin/env dsc
# module alias and executables
simulate: datamaker.R
    # module input and variables
    mixcompdist: raw(normalmix)
    g: (raw(c(2/3,1/3)),raw(c(0,0)),raw(c(1,2))),
       (raw(rep(1/7,7)),raw(c(-1.5,-1,-0.5,0,0.5,1,1.5)),raw(rep(0.5,7))),
       (raw(c(1/4,1/4,1/3,1/6)),raw(c(-2,-1,0,1)),raw(c(2,1.5,1,1)))
    min_pi0: 0
    max_pi0: 1
    nsamp: 1000
    betahatsd: 1
    # module decoration
    @ALIAS: args = List()
    # module output
    $data: data
    $true_beta: data$meta$beta
    $true_pi0: data$meta$pi0

shrink: runash.R
    # module input and variables
    input: $data
    mixcompdist: normal, halfuniform
    # module output
    $ash_data: ash_data
    $beta_est: ashr::get_pm(ash_data)
    $pi0_est: ashr::get_pi0(ash_data)

score_beta: score.R
    # module input and variables
    est: $true_beta
    truth: $beta_est
    # module output aka pipeline variable
    $mse: result

score_pi0: score.R
    # module input and variables
    est: $pi0_est
    truth: $true_pi0
    # module output
    $mse: result

DSC:
    # module ensembles
    define:
      score: score_beta, score_pi0
    # pipelines
    run: simulate * shrink * score
    # runtime environments
    R_libs: ashr (2.2.7+)
    exec_path: bin
    output: dsc_result
    # pipeline variables, will override any module variables of the same name
    # it is also place to config the global random number generator
