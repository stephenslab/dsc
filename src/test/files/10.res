DSC:
  output:
  - dsc_result
  run:
  - (tuple)
    - SVA
  - (tuple)
    - RUV
  - (tuple)
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
    - args = Pack()

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
    - args = Pack()

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
    - args = Pack()
