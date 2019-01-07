#!/usr/bin/env dsc

simulate: R(library(mashr)) + simulate_*@intro_mash.Rmd
  n_effects: 500
  n_cond: 5
  $data: data

get_cov: R(library(mashr)) + cov*@intro_mash.Rmd
  data: $data
  $U_c: U.c 

fit: R(library(mashr)) + fit*@intro_mash.Rmd
  U_c: $U_c
  data: $data
  $m_c: m.c
  @ALIAS: U.c = U_c
 
DSC:
  run: simulate * get_cov * fit
  R_libs: mashr@stephenslab/mashr (>=0.2.6)
  output: mash_result
