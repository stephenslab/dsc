# This is the main simulator file

library(simulator) # this file was created under simulator version 0.2.0

source("model_functions.R")
source("method_functions.R")
source("eval_functions.R")

## @knitr init

name_of_simulation <- "normal-mean-estimation-with-contamination"

## @knitr main

sim <- new_simulation(name = name_of_simulation,
                      label = "Mean estimation under contaminated normal") %>%
  generate_model(make_my_model, seed = 123,
                 n = 50,
                 prob = as.list(seq(0, 1, length = 6)),
                 vary_along = "prob") %>%
  simulate_from_model(nsim = 10) %>%
  run_method(list(my_method, their_method)) %>%
  evaluate(list(his_loss, her_loss))

## @knitr plots

plot_eval_by(sim, "hisloss", varying = "prob")

## @knitr tables

tabulate_eval(sim, "herloss", output_type = "markdown",
              format_args = list(digits = 1))
