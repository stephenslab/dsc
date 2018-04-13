# Dynamic Statistical Comparisons (DSC)

[![PyPI version](https://badge.fury.io/py/dsc.svg)](https://badge.fury.io/py/dsc)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/46bb573ea0414f6095f1b7fd4bedbfd3)](https://www.codacy.com/app/gaow/dsc?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=stephenslab/dsc&amp;utm_campaign=Badge_Grade)

The [project wiki](https://stephenslab.github.io/dsc-wiki) is the main source of documentation for both developers and users of the DSC project. If you are new to the concept of DSC, it may worth reading this [blog post](http://stephens999.github.io/blog/2014/10/Data-Driven-Discovery.html) to understand the motivation behind this project.

This work is supported by the the Gordon and Betty Moore Foundation via an Investigator Award to Matthew Stephens, [Grant GBMF4559](https://www.moore.org/grants/list/GBMF4559), as part of the [Data-Driven Discovery program](https://www.moore.org/programs/science/data-driven-discovery). If you have any questions or want to share some information with the developer / user community, please open a [github issue](https://github.com/stephenslab/dsc/issues).

## Change Log

### Upcoming release

Goal for 0.2.8

* Support for multiple outputs per module, for shell executables
* Improve command options for scripts

### 0.2.x

0.2.7.3

* [minor] Fix a regression bug on path due to 0.2.7.2.
* [minor] Check and properly quit on error when `Rscript` command is not found for R modules.

0.2.7.2

* Improved Windows path support.
* [minor] Fix a bug with nested tuple with `raw()`.

0.2.7.1

* Dump individual data object with scripts using `dsc-query *.pkl` and `dsc-query *.rds`.
* [minor] Improve behavior for length 1 vector in R's list with `R()` operator.
* [minor] Various bug fixes.

0.2.7

* [#92](https://github.com/stephenslab/dsc/issues/92) paired parameter input convention.
* [#90](https://github.com/stephenslab/dsc/issues/90) and [#93](https://github.com/stephenslab/dsc/issues/93) use `Rmd` files as module executables.
* [#94](https://github.com/stephenslab/dsc/issues/94) and [#95](https://github.com/stephenslab/dsc/issues/95) added `DSC::replicate` and command option `--replicate`.
* Enhance `R()` operator due to use of [dscrutils](https://github.com/stephenslab/dsc/tree/master/dscrutils) package. This packages is now required to parse DSC file when `R` modules are involved.
* Add, by default, a variable `DSC_DEBUG` to output files that saves various runtime info.
* SoS bumped to 0.9.13.2
  * Support R github package force install when version mismatches.
  * Fix bug with `--touch` option.
  * Force use `pip` to install local development version.
  * [#97](https://github.com/stephenslab/dsc/issues/97) Improved error logging and reporting behavior.
* [minor] Revert from `ruamel.yaml` to `yaml` for better performance.
* [minor] [#96](https://github.com/stephenslab/dsc/issues/96), [#98](https://github.com/stephenslab/dsc/issues/98)
* [minor] Various bug fixes.

0.2.6.5

* Bring back partial mixed languages support. **Piplines with mixed R and Python code can communicate data of limited types (recursively support array, matrix, dataframe), via `rpy2` as in versions prior to 0.2.5.x**. Support for additional languages will be implemented on need basis with `HDF5` format [#86](https://github.com/stephenslab/dsc/issues/86).

0.2.6.4

* Add a `dsc-io` command to convert between python `pickle` and R `RDS` files -- an internal command for data conversion and a test for `rpy2` configuration.

0.2.6.3

* Inline module executable via language interpreters (eg. `R()`, `Python()`).

0.2.6.2

* [minor] Ignore leading `.` in `file()`: `file(.txt)` and `file(txt)` are equivalent.
* [minor] Disallow derivation of modules from ensemble.
* [minor] Various bug fixes.

0.2.6.1

* Internally replace `RDS` format with `HDF5` format for Python routines. **Pipeline with mixed languages is now officially broken at this point until the next major release that supports `HDF5` in R**.
* SoS required version bumped to 0.9.12.7 for relevant upstream bug fixes for remote host computing.
* [minor] Various bug fixes.

0.2.6

* Bring back `--host` option; add a companion option `--to-host` to facilicate sending resources to remote computer.
* Add `--truncate` switch.
* SoS required version bumped to 0.9.12.3 for relevant upstream bug fixes.
* [minor] Improved command interface.

0.2.5.2

* SoS required version bumped to 0.9.12.2 for relevant upstream bug fixes.

0.2.5.1

* Change in `seed` behavior: since this release `seed` will no longer be a DSC keyword. Users are responsible to set seeds on their own.
* [minor] Allow for both lower case and capitalized operator names `File/file, List/list, Dict/dict`.

0.2.5

* New syntax release, compatible with SoS 0.9.12.1.
* Removed `--host` option due to upstream changes.

### 0.1.x

0.1.0

* First release, compatible with SoS 0.6.4.
