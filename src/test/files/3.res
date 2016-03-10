DSC:
  run:
  - simulate
  runtime:
    output:
    - files/3

simulate:
  meta:
    exe:
    - (tuple)
      - datamaker.R
      - (tuple)
        - 1
        - 2
        - 3
        - 4
  params:
    0: {}
  return:
  - data
