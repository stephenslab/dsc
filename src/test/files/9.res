DSC:
  run:
  - (tuple)
    - simulate
    - method
  runtime:
    output:
    - files/9

method:
  meta:
    exe:
    - (tuple)
      - deseq2.wrapper.R
    - (tuple)
      - edger.wrapper.R
    - (tuple)
      - limma.wrapper.R
    - (tuple)
      - ash.wrapper.R
    - (tuple)
      - jointash.wrapper.R
  params:
    0:
      input:
      - $data
      transform:
      - voom
      - RUVvoom
      - SVAvoom
      - quasibinom
      - RUV+quasibinom
      - SVA+quasibinom
      - Myrna+quasibinom
      - edgeRglm
    2:
      RUV:
      - 'FALSE'
      - 'FALSE'
      - 'TRUE'
      SVA:
      - 'FALSE'
      - 'TRUE'
      - 'FALSE'
      exacttest:
      - 'TRUE'
      - 'FALSE'
      input:
      - $data
      transform:
      - voom
      - RUVvoom
      - SVAvoom
      - quasibinom
      - RUV+quasibinom
      - SVA+quasibinom
      - Myrna+quasibinom
      - edgeRglm
    3:
      input:
      - $data
      robust:
      - 'FALSE'
      - 'TRUE'
      transform:
      - voom
      - RUVvoom
      - SVAvoom
      - quasibinom
      - RUV+quasibinom
      - SVA+quasibinom
      - Myrna+quasibinom
      - edgeRglm
    5:
      input:
      - $data
      singlecomp:
      - true
      transform:
      - voom
      - RUVvoom
      - SVAvoom
      - quasibinom
      - RUV+quasibinom
      - SVA+quasibinom
      - Myrna+quasibinom
      - edgeRglm
  params_alias:
    0:
    - RList(args, transform)
    2:
    - args = RList()
    3:
    - args = RList()
    5:
    - args = RList()
  return:
  - output
  rules:
    2:
    - exacttest[1]
    - (tuple)
      - exacttest[2]
      - RUV+SVA
    3:
    - (tuple)
      - transform[1]
      - robust
    - (tuple)
      - transform[2,3]
      - robust[1]
    4:
    - transform[1:6]

simulate:
  meta:
    exe:
    - (tuple)
      - datamaker.R
    seed:
    - (tuple)
      - 1
      - 2
      - 3
      - 4
      - 5
      - 6
      - 7
      - 8
      - 9
      - 10
      - 11
      - 12
      - 13
      - 14
      - 15
      - 16
      - 17
      - 18
      - 19
      - 20
      - 21
      - 22
      - 23
      - 24
      - 25
      - 26
      - 27
      - 28
      - 29
      - 30
      - 31
      - 32
      - 33
      - 34
      - 35
      - 36
      - 37
      - 38
      - 39
      - 40
      - 41
      - 42
      - 43
      - 44
      - 45
      - 46
      - 47
      - 48
      - 49
      - 50
    - (tuple)
      - 1
      - 2
      - 3
      - 4
      - 5
  params:
    0:
      Ngene:
      - 10000
      Nsamp:
      - 2
      - 10
      - 50
      breaksample:
      - 'FALSE'
      - 'TRUE'
      nullpi:
      - 0.9
      path:
      - file.txt
      poisthin:
      - true
      tissue:
      - Adipose-Subcutaneous
      - Lung
      voom.normalize:
      - true
  params_alias:
    0:
    - args = RList()
  return:
  - data
  rules:
    0:
    - (tuple)
      - seed[1]
      - path
      - tissue
      - Nsamp
      - Ngene
      - voom.normalize
      - breaksample
    - (tuple)
      - seed[2]
      - path
      - tissue[2]
      - Nsamp
      - Ngene
      - voom.normalize
      - breaksample[2]
      - nullpi
    - (tuple)
      - seed[2]
      - path
      - tissue[1]
      - Nsamp
      - Ngene
      - voom.normalize
      - breaksample[1]
      - poisthin
