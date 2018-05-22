# Dynamic Statistical Comparisons (DSC)

[![PyPI version](https://badge.fury.io/py/dsc.svg)](https://badge.fury.io/py/dsc)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/46bb573ea0414f6095f1b7fd4bedbfd3)](https://www.codacy.com/app/gaow/dsc?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=stephenslab/dsc&amp;utm_campaign=Badge_Grade)

The [project wiki](https://stephenslab.github.io/dsc-wiki) is the main source of documentation for both developers and users of the DSC project. If you are new to the concept of DSC, it may worth reading this [blog post](http://stephens999.github.io/blog/2014/10/Data-Driven-Discovery.html) to understand the motivation behind this project.

This work is supported by the the Gordon and Betty Moore Foundation via an Investigator Award to Matthew Stephens, [Grant GBMF4559](https://www.moore.org/grants/list/GBMF4559), as part of the [Data-Driven Discovery program](https://www.moore.org/programs/science/data-driven-discovery). If you have any questions or want to share some information with the developer / user community, please open a [github issue](https://github.com/stephenslab/dsc/issues).

## Developer notes

### Upgrading DSC to latest development version

For most users, we recommend installing the [most recent stable
release](https://stephenslab.github.io/dsc-wiki/installation.html). If
you would like to upgrade your existing installation of DSC to the
most recent (unstable) development version, follow these steps.

DSC is closely developed in parallel with
[SoS](http://github.com/vatlab/sos). Therefore, the development
version of DSC (maintained in the `master` branch of the GitHub
repository) typically requires the development version of SoS.

#### Install the latest SoS

```
git clone https://github.com/vatlab/SoS.git
cd SoS
pip install -U --upgrade-strategy only-if-needed . 
```

#### Install DSC from source

```
git clone https://github.com/stephenslab/dsc.git
cd dsc
./setup.sos
```

Or, if you have downloaded it before,

```
cd dsc
git pull
./setup.sos
```

#### Install dscrutils from source

Assuming the working directory in your R environment is the `dsc`
repository, run the following code in R to install the latest
development version of the dscrutils R package:

```r
getwd() # Should be ... /dsc
install.packages("dscrutils",repos = NULL,type = "source")
```

#### Project maintenance

Although relatively stable and usable in practice, DSC is still actively being developed.
Occasionally upgrades to the most recent version will lead to changes of file signatures that 
triggers rerun of existing benchmark even if they have not been changed. When this happens we will
indicate in bold in our release note below that "a file signature clean up is recommended" 
(see release note 0.2.7.7 for example). That means after such DSC upgrades you should 
rerun your benchmark with `--touch` option to update file signatures. If possible, it is recommended 
that you rerun your benchmark from scratch (if resources can afford) with `-s none` instead of `--touch` 
to skip all existing files. We apologize for the inconveniences it incurs. 

## Change Log

### Upcoming release

Goal for 0.2.9

- Support for multiple output per modules.
- Improve scripts command options.

### 0.2.x

0.2.8.3

- `dsc-io` can now convert CSV to HTML with pop-up figures.

0.2.8.2

- Add `-p` option to print stdout and stderr to screen.
- SoS bumped to version 0.9.14.1 for 
  - Improved parallel slot management.
  - Improved messaging on executed steps (use `-v 3` to display in DSC).

0.2.8.1

- Minor file check performance optimization.
- Force overwrite converted `pkl` to `rds` in `dscutils::dscquery`, as a save default.

0.2.8

Input string parameter behavior has changed since this version. Now un-quoted strings will be treated
input script code; string parameters will have to be quoted. A new DSC configuration parser has been
implemented to overcome `pyYAML` restrictions. Please submit a bug report if the new parser misbehaves.

**A file signature clean up is recommended after this upgrade.**

0.2.7.11

- [minor] More stringent check on improper module names ending with `_{digits}`.

0.2.7.10

- Stop adding script hash to default seed #136.
- [minor] SoS bumped to version 0.9.13.8 a bug fix release.

**A file signature clean up is recommended after this upgrade.**

0.2.7.9

Minor touches on 0.2.7.8 -- just a celebration of the 1,000-th commit to the DSC repo on github,
after 2 years and 3 months into this project.

0.2.7.8

- Implement a preliminary `%include` feature to provide alternative code organization style.
- Allow for `!` operator in `List()` and `Dict()`.
- SoS bumped to version 0.9.13.7 for improved remote job support.
- [minor] Various bug fixes.

0.2.7.7

- Improvements for module with shell executables and command options.
- Improvements for remote execution #131.
- Improved logging.
- Bug fixes #126, #127.
- SoS bumped to version 0.9.13.4 for #128 and related.

**A file signature clean up is recommended after this upgrade.**

0.2.7.6

- Add new feature `dscrutils::shiny_plot` to display simple benchmark results.
- [minor] Display unused modules with `-h` option.

0.2.7.5

- Add R / Python packages and version display with `-h` option.
- Add `.gitignore` for cache folder when a git environment is detected.
- SoS bumped to 0.9.13.3 that now bundles the `pbs` module.

0.2.7.4

- Improved R's sessionInfo format.
- Bug fixes #119, #121, #122
- [minor] Error message improvements.

0.2.7.3

- More stringent R library and command executable check.
- [minor] Fix a regression bug on path due to 0.2.7.2.

0.2.7.2

- Improved Windows path support.
- [minor] Fix a bug with nested tuple with `raw()`.

0.2.7.1

- Dump individual data object with scripts using `dsc-query *.pkl` and `dsc-query *.rds`.
- [minor] Improve behavior for length 1 vector in R's list with `R()` operator.
- [minor] Various bug fixes.

0.2.7

- [#92](https://github.com/stephenslab/dsc/issues/92) paired parameter input convention.
- [#90](https://github.com/stephenslab/dsc/issues/90) and [#93](https://github.com/stephenslab/dsc/issues/93) use `Rmd` files as module executables.
- [#94](https://github.com/stephenslab/dsc/issues/94) and [#95](https://github.com/stephenslab/dsc/issues/95) added `DSC::replicate` and command option `--replicate`.
- Enhance `R()` operator due to use of [dscrutils](https://github.com/stephenslab/dsc/tree/master/dscrutils) package. This packages is now required to parse DSC file when `R` modules are involved.
- Add, by default, a variable `DSC_DEBUG` to output files that saves various runtime info.
- SoS bumped to 0.9.13.2
	- Support R github package force install when version mismatches.
	- Force use `pip` to install local development version.
	- [#97](https://github.com/stephenslab/dsc/issues/97) Improved error logging and reporting behavior.
- [minor] Revert from `ruamel.yaml` to `yaml` for better performance.
- [minor] [#96](https://github.com/stephenslab/dsc/issues/96)
- [minor] [#98](https://github.com/stephenslab/dsc/issues/98)
- [minor] Various bug fixes.

0.2.6.5

- Bring back partial mixed languages support. **Piplines with mixed R and Python code can communicate data of limited types (recursively support array, matrix, dataframe), via `rpy2` as in versions prior to 0.2.5.x**. Support for additional languages will be implemented on need basis with `HDF5` format [#86](https://github.com/stephenslab/dsc/issues/86).

0.2.6.4

- Add a `dsc-io` command to convert between python `pickle` and R `RDS` files -- an internal command for data conversion and a test for `rpy2` configuration.

0.2.6.3

- Inline module executable via language interpreters (eg. `R()`, `Python()`).

0.2.6.2

- [minor] Ignore leading `.` in `file()`: `file(.txt)` and `file(txt)` are equivalent.
- [minor] Disallow derivation of modules from ensemble.
- [minor] Various bug fixes.

0.2.6.1

- Internally replace `RDS` format with `HDF5` format for Python routines. **Pipeline with mixed languages is now officially broken at this point until the next major release that supports `HDF5` in R**.
- SoS required version bumped to 0.9.12.7 for relevant upstream bug fixes for remote host computing.
- [minor] Various bug fixes.

0.2.6

- Bring back `--host` option; add a companion option `--to-host` to facilicate sending resources to remote computer.
- Add `--truncate` switch.
- SoS required version bumped to 0.9.12.3 for relevant upstream bug fixes.
- [minor] Improved command interface.

0.2.5.2

- SoS required version bumped to 0.9.12.2 for relevant upstream bug fixes.

0.2.5.1

- Change in `seed` behavior: since this release `seed` will no longer be a DSC keyword. Users are responsible to set seeds on their own.
- [minor] Allow for both lower case and capitalized operator names `File/file, List/list, Dict/dict`.

0.2.5

- New syntax release, compatible with SoS 0.9.12.1.
- Removed `--host` option due to upstream changes.

### 0.1.x

0.1.0

- First release, compatible with SoS 0.6.4.
