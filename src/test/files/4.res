DSC:
  output:
  - dsc_result
  run:
  - (tuple)
    - outlier_test
  work_dir:
  - ./

outlier_test:
  meta:
    exec:
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
  out:
  - data
  - score = R(data$score)
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
    - args = Pack()
