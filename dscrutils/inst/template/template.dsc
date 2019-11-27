#!/usr/bin/env dsc

#
# example usage: ./template.dsc --replicate 10
#

%include modules/*.dsc

DSC:
  define:
    simulate: normal
    analyze: mean
    score: sq_err
  run: simulate * analyze * score
  output: template_out
  exec_path: src
