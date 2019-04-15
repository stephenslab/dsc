#!/usr/bin/env dsc

# A very short DSC used to test storage and querying of module
# parameters and outputs that are NULL.

foo: R(if (is.null(a)) a <- 1; x <- a*rnorm(1))
  a: NULL, 4
  $data: x

bar: R(x <- rnorm(1); if (x < 0) x <- NULL)
  $data: x

DSC:
  replicate: 4
  define:
    simulate: foo, bar
  run: simulate
