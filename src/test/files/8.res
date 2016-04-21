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
      - datamaker.R
  out:
  - data
  params:
    0:
      tissue:
      - (tuple)
        - Adipose-Subcutaneous
        - Lung
      - Lung
