DSC:
  output:
  - dsc_result
  run:
  - (tuple)
    - simulate
  work_dir:
  - ./

simulate:
  meta:
    exec:
    - (tuple)
      - rnorm.R
    - (tuple)
      - rt.R
    seed:
    - 1
    - 2
    - 3
    - 4
    - 5
  out:
  - x
  - true_mean
  params:
    0:
      n:
      - 1000
      true_mean:
      - 0
      - 1
