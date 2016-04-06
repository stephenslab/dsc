DSC:
  output:
  - dsc_result
  run:
  - SVA
  - RUV
  - myrna
  work_dir:
  - ./

RUV:
  meta:
    exec:
    - (tuple)
      - RUV.R
  out:
  - data
  params:
    0:
      data:
      - $data
  params_alias:
    0:
    - args = RList()

SVA:
  meta:
    exec:
    - (tuple)
      - SVA.R
  out:
  - data
  params:
    0:
      data:
      - $data
  params_alias:
    0:
    - args = RList()

myrna:
  meta:
    exec:
    - (tuple)
      - myrna.R
  out:
  - data
  params:
    0:
      data:
      - $data
  params_alias:
    0:
    - args = RList()
