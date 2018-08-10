simulate: R()
  $data: 1

cause: R()
  data: $data
  z:    0.25
  $res: 1:10

sim_params: R()
  data: $data
  $params_store: 1:20

DSC:
  run: simulate*cause, simulate*sim_params
  replicate: 2
  output: results2
  exec_path: R
