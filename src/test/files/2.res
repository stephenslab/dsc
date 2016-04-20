DSC:
  output:
  - dsc_result
  run:
  - simulate
  work_dir:
  - ./

simulate:
  meta:
    exec:
    - (tuple)
      - datamaker.R
  out:
  - data
  - true_beta = R(data$meta$beta)
  params:
    0:
      betahatsd:
      - 1
      max_pi0:
      - 1
      min_pi0:
      - 0
      nsamp:
      - 1000
  params_alias:
    0:
    - args = Pack()
