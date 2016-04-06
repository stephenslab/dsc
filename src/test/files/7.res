DSC:
  output:
  - dsc_result
  parameters:
    data_functions:
    - mvngenotypes
    - discrete.cosine
    - discrete.cosine2
    - discrete.cosine.peaksel
  run:
  - simulate
  work_dir:
  - ./

simulate:
  meta:
    exec:
    - (tuple)
      - datamaker.R
      - mvngenotypes
    - (tuple)
      - datamaker.R
      - discrete.cosine
  out:
  - data
  params:
    0:
      n.neutral.snps:
      - 9500
