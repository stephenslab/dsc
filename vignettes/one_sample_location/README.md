# One sample location vignette

This vignette illustrates the "simulate-analyze-score" design pattern
for a very simple task: estimate the mean of a population given a
random sample from the population. The examples in this folder are
used in the introductory tutorials.

## Setup instructions

To run the DSC examples on your computer, follow these setup instructions.

1. Install [DSC](https://stephenslab.github.io/dsc-wiki/installation.html).

2. Install [R](http://cran.r-project.org).

3. Install the `dscrutils` R package. See
   [here](https://stephenslab.github.io/dsc-wiki/installation.html)
   for instructions.

4. Make sure you are able to run the R executable front-end `Rscript`
   from the command-line shell. If you run

   ```bash
   Rscript --version
   ```

   and you do not get a version number, but rather an error similar to
   "command not found", then you will need to add the install location
   of Rscript to the `PATH` environment variable. Running this code
   inside R or RStudio should give the location of Rscript:

   ```R
   file.path(R.home(),"bin")
   ```

*NOTE: Later we will include a configuration setting for the Rscript
executable, in which case we should add these details to Step 4.*

## What's included

+ `first_investigation.dsc`: example used in the
  [Introduction](https://stephenslab.github.io/dsc-wiki/tutorials/Intro_DSC.html).

+ `first_investigation_simpler.dsc`: simpler version of
  `first_investigation.dsc` used in
  [DSC Basics, Part I](https://stephenslab.github.io/dsc-wiki/tutorials/Intro_Syntax_I.html).

+ `add_winsor_method1.dsc`: This is the same as
  `first_investigation.dsc`, except that another method for estimating
  the population mean is included. This example is also used in
  introductory tutorial.
