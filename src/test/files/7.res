DSC:
  run:
  - simulate
  runtime:
    output:
    - files/7

simulate:
  meta:
    exe:
    - (tuple)
      - datamaker.R
      - mvngenotypes
    - (tuple)
      - datamaker.R
      - discrete.cosine
  params:
    0:
      n.neutral.snps:
      - 9500
  return:
  - data
