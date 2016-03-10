DSC:
  run:
  - outlier_test
  runtime:
    output:
    - files/4

outlier_test:
  meta:
    exe:
    - (tuple)
      - method1.R
    - (tuple)
      - method2.R
    - (tuple)
      - method3.R
    - (tuple)
      - method4.R
    - (tuple)
      - method5.R
    - (tuple)
      - method6.R
  params:
    3:
      K:
      - 2
      - 5
    4:
      K:
      - 2
      - 5
    5:
      K:
      - 2
      - 3
    6:
      K:
      - 3
  params_alias:
    0:
    - args = RList()
  return:
  - data
  - score = R(data$score)
