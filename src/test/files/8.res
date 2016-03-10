DSC:
  run:
  - simulate
  runtime:
    output:
    - files/8

simulate:
  meta:
    exe:
    - (tuple)
      - datamaker.R
  params:
    0:
      tissue:
      - (tuple)
        - Adipose-Subcutaneous
        - Lung
      - Lung
  return:
  - data
