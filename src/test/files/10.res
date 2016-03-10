DSC:
  run:
  - SVA
  - RUV
  - myrna
  runtime:
    output:
    - files/10

RUV:
  meta:
    exe:
    - (tuple)
      - RUV.R
  params:
    0:
      data:
      - $data
  params_alias:
    0:
    - args = RList()
  return:
  - data

SVA:
  meta:
    exe:
    - (tuple)
      - SVA.R
  params:
    0:
      data:
      - $data
  params_alias:
    0:
    - args = RList()
  return:
  - data

myrna:
  meta:
    exe:
    - (tuple)
      - myrna.R
  params:
    0:
      data:
      - $data
  params_alias:
    0:
    - args = RList()
  return:
  - data
