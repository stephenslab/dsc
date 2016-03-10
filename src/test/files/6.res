DSC:
  run:
  - simulate
  runtime:
    output:
    - files/6

simulate:
  meta:
    exe:
    - (tuple)
      - datamaker.R
  params:
    0:
      g:
      - Asis(normalmix(c(2/3,1/3),c(0,0),c(1,2)))
      - Asis(normalmix(rep(1/7,7),c(-1.5,-1,-0.5,0,0.5,1,1.5),rep(0.5,7)))
  params_alias:
    0:
    - args = RList()
  return:
  - data
