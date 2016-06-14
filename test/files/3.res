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
      - 1
    - (tuple)
      - datamaker.R
      - 2
    - (tuple)
      - datamaker.R
      - 3
    - (tuple)
      - datamaker.R
      - 4
  out:
  - data
