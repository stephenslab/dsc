DSC:
  run:
  - simulate
  runtime:
    output:
    - files/1

simulate:
  meta:
    exe:
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
  params:
    0:
      n:
      - 1000
      true_mean:
      - 0
      - 1
  return:
  - x
  - true_mean
