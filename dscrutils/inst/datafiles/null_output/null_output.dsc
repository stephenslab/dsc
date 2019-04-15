# A very short DSC used to test storage and querying of module
# parameters and outputs that are NULL.

foo: R(a <- ifelse(is.null(a),1,a); x <- a*rnorm(1))
  a: NULL, sqrt(2)
  $data: x

bar: R(y <- rnorm(1); x <- ifelse(y > 0,y,NULL))
  $data: x

DSC:
  replicate: 10
  define:
    simulate: foo, bar
  run: simulate
