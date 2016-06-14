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
    seed:
    - 1
    - 2
    - 3
    - 4
    - 5
  out:
  - data
  params:
    0:
      angle:
      - 0.0
      - 0.3927
      - 0.7854
      - 1.1781
      - 1.5708
