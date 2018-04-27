#!/usr/bin/env dsc

a: R(x <- rnorm(1))
  $x: abs(x)
  r: 0

b: R(x <- rt(1,df = 4))
  m: 0
  $r: x
  $x: abs(x)
  
c: R(x <- m*runif(1))
  m: 1, 2, 3
  $x: 2*x

t1: R(x <- sqrt(x + 1e-6))
  x: $x
  $x: x

t2: R(x <- x^2)
  x: $x
  $x: x

DSC:
  define:
    ab: a, b
    ac: a, c
  run: (ab * t1), (ac * t2)

