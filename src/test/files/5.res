DSC:
  run:
  - simulate
  runtime:
    output:
    - files/5

simulate:
  meta:
    exe:
    - (tuple)
      - datamaker.R
    seed:
    - 1
    - 2
    - 3
    - 4
    - 5
  params:
    0:
      angle:
      - 0.0
      - 0.39269908169872414
      - 0.7853981633974483
      - 1.1780972450961724
      - 1.5707963267948966
  return:
  - data
