simulate: R()
  $data: 1

cause: R()
  data: $data
  z:    0.25
  $res: 1:10

sim_params: R()
  data: $data
  $params_store: 5

DSC:
  run: simulate*cause, simulate*sim_params
  replicate: 2
  output: results1
  exec_path: R
