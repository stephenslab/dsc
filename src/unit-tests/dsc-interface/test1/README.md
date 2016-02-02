## Example: Location Parameter Estimate
### DSC setup
This minimal example shows comparison of location parameter estimation methods. Below is a list of contents involved:

```
  ├── R
  │   ├── methods
  │   │   ├── mean.R
  │   │   └── median.R
  │   ├── scenarios
  │   │   ├── rnorm.R
  │   │   └── rt.R
  │   └── scores
  │       └── MSE.R
  └── settings.yaml
```

Contents of R scripts are:

```
  ==> ../src/unit-tests/dsc-interface/test1/R/scores/MSE.R <==
  mse = (mean_est-true_mean)^2
  
  ==> ../src/unit-tests/dsc-interface/test1/R/scenarios/rnorm.R <==
  # produces n random numbers from normal with specified mean
  x=rnorm(n,mean=mean)
  
  ==> ../src/unit-tests/dsc-interface/test1/R/scenarios/rt.R <==
  # produces n random numbers from t with df=2 and  with specified mean
  x=mean+rt(n,df=2)
  
  ==> ../src/unit-tests/dsc-interface/test1/R/methods/median.R <==
  mean = median(x)
  
  ==> ../src/unit-tests/dsc-interface/test1/R/methods/mean.R <==
  mean = mean(x)
  
  
```

And the dsc configuration file  `settings.yaml`:

```
  scenario:
    exe: [R/scenarios/rnorm.R, R/scenarios/rt.R]
    n: 1000
    mean: [0, 1]
    seed: [1, 2, 3, 4, 5]
    return: [x, mean]
  
  method:
    exe: [R/methods/mean.R, R/methods/median.R]
    x: $scenario.x
    return: mean
  
  score:
    exe: R/scores/MSE.R
    mean_est: $method.mean
    true_mean: $scenario.mean
    return: mse
  
  runtime:
    lib_path:
    bin_path:
    output: simple
  
```

A basic configuration file should have 4 sections: `scenario`, `method`, `score`, and `runtime`. The first 3 sections **require** two **reserved** parameters: `exe` and `return`. In addition to these keywords one need to specify the parameters involved in running the benchmarking commands. All parameters in the `runtime` section are reserved.

### Run DSC
```
dsc execute -c settings.yaml
```

```
  MESSAGE: Setup scenarios ...
  MESSAGE: Apply methods ...
  MESSAGE: Compute scores ...
```

3 files are be produced in the process:

```
  simple_method.h5  simple_scenario.h5  simple_score.h5
```

Contents of these files can be examined via `dsc show` or visualized via `dsc viz` ... (TBA) ...
