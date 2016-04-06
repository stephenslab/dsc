DSC:
  output:
  - dsc_result
  run:
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
      - 0.39269908169872414
      - 0.7853981633974483
      - 1.1780972450961724
      - 1.5707963267948966
