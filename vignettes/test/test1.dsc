#!/usr/bin/env dsc

a: R(x <- rnorm(1))
  $x: abs(x)

b: R(x <- rt(1,df = 4))
  $x: abs(x)
  
c: R(x <- m*runif(1))
  m: 1, 2, 3
  $x: 2*x
  
sqerr: R(mse <- mean((A %*% x - b)^2))
  A: $A
  x: $x
  b: $b
  $error: mse

DSC:
  define:
    gen: a, b, c
  run: gen
