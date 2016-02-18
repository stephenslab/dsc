# DSCR for outlier detections from population genetic data
A DSCR (https://github.com/stephens999/dscr) is a set of R scripts with the goal
of creating scenarios to evaluate different statistical methods for the same 
purpose. The project is developed by Matthew Stephens at the University of
Chicago. While it is still in development, and has some drawbacks, I think it's
 a great tool, because

1. Writing testing frameworks for methods is neither fun nor rewarding
2. All to often, simulation scenarios accompanying a software release are
very favorable to the authors methods.

Having an 'objective', or at least
'canonical' set of simulations can help with this.

## Objectives
The goal here is to evaluate methods that look at outliers under complex 
population genetic structure model. Such loci are of interest because they
may be significant in evolutionary history, and may be candidates for 
local adaptation.

Right now, the following methods are implemented:

- FST based outliers (Lewontin \& Krakauer 1973)
- FLK (Bonhomme et al. 2010), using NJ tree
- FLK from empirical covariance, also called XtX (Bonhomme et al. 2010)
- Logistic factor analysis (https://github.com/StoreyLab/lfa) outliers


## Installation
The project requires the following packages:

    - mvtnorm (CRAN)
    - ape (CRAN)
    - lfa (https://github.com/StoreyLab/lfa)
    - dscr (https://github.com/stephens999/dscr)
    - MASS (recommended)
    - Matrix (recommended)

## Run

    source("dsc_pcasel.r")
    r <- run_dsc(dsc_pcasel)

    # do whatever with output

## Overview

### Datamakers
the main simulator (as "datamakers") is the `datamaker` function,
which follows the dscr function syntax. It calls more specialised
functions and handles the conversion from allele frequencies to 
genotype. 


##### datamaker.mvngenotypes
Simualtes genotypes from random locations in the unit square, and 
then generates genotypes based on a distance matrix

##### datamaker.discrete.cosine
Simualtes genotypes from a spatial model by constructing a covariance
matrix from the basis of the discrete cosine transform. The reason for this
approach is detailed in the Novembre \& Stephens (2008) Nature Genetics
paper. Selection is introduced by increasing the coefficient for one of the
two leading eigenvalues. `datamaker.discrete.cosine2` is the same function
with a different parameterization.

##### datamaker.discrete.cosine.peaksel
Same model as above, but with selection introduced by increasing
genotype in a small region, for local adaptation on a spatial
background.

### Input
Each Datamaker returns, (and each method reads) a list with the following two components
 -raw the `n x p ` matrix of SNP, each entry should be in `(0,1,2)`.
 -svd Singular value decomposition of raw.

### Methods
The following methods are implemented:

##### method.duforet.rho
Rho method of Duforet et al. corresponding to the first eigenvalues of PCA. 
Identical to the statistic in Galinksy et al.

- args K: the K-th PC

##### method.duforet.rho1
first PC
##### method.duforet.rho2
second PC
##### method.duforet.h
weighted sum of loadings
##### method.duforet.hprime
unweighted sum of loadings
##### method.lfa
Deviance from logistic factor analysis from storey at al. paper
unweighted sum of loadings
##### method.gwish
Genearlized Wishart distribution after McCullagh 2009, identical (up to 
normalization) to Likelihood from Bonhomme et al. 2010 and Guenther \& Coop 2013

##### method.gwish.tree
Bonhomme et al. 2010 likelihood with NJ tree

##### method.fst
FST method by Lewontin \& Krakauer 1973


### Scores
##### top100
find the number of true positives in the best 100 SNP.
