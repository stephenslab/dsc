## Reimplementation of "quick start" example from `simulator` R package

This example reimplements the [Getting Started with Simulator](http://faculty.bscb.cornell.edu/~bien/simulator_vignettes/getting-started.html) example. Only the execution part is implemented in DSC. One has to use `dscrutils` for data exploration.

The example can be executed in DSC via:

```
dsc main.dsc
```

To reproduce in `simulator` package:

```
library(simulator)
dir <- "./sims"
create(dir)
setwd(dir)
rmarkdown::render("writeup.Rmd", "html_document")
```

The code for `simulator` package are uploaded to `sims` folder. 
Here are the [`writeup.Rmd`](sims/writeup.Rmd) and [`writeup.html`](sims/writeup.html) from `simulator` run above.
