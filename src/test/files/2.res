DSC:
  run:
  - simulate
  runtime:
    output:
    - files/2

simulate:
  meta:
    exe:
    - (tuple)
      - datamaker.R
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
    - args = RList()
  return:
  - data
  - true_beta = R(data$meta$beta)
