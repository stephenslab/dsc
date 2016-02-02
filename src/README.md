## Installation
### Binary releases
TBA

### Source compile
TBA

### Interface preview
You can test your `dsc2` installation by viewing its interface via `dsc -h`:

```
  usage: dsc [-h] {execute,show} ...
  
  Implementation of Dynamic Statistical Comparisons
  
  positional arguments:
    {execute,show}
      execute       Execute DSC benchmark
      show          Explore DSC benchmark data
  
  optional arguments:
    -h, --help      show this help message and exit
  
```

and `dsc execute -h`:

```
  usage: dsc execute [-h] -c CONFIG_FILE [-v {0,1}]
  
  optional arguments:
    -h, --help            show this help message and exit
    -c CONFIG_FILE        DSC benchmark settings (default: None)
    -v {0,1}, --verbosity {0,1}
                          Verbosity level (default: 1)
  
```
