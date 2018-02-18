# DSC2
DSC2 is successor of the [Dynamic Statistical Comparisons in R](https://github.com/stephens999/dscr).

The [project wiki](https://stephenslab.github.io/dsc-wiki) is the main source of documentation for both developers and users of the DSC2 project. If you are new to the concept of DSC, it may worth reading this [blog post](http://stephens999.github.io/blog/2014/10/Data-Driven-Discovery.html) to understand the motivation behind this project.

This work is supported by the the Gordon and Betty Moore Foundation via an Investigator Award to Matthew Stephens, [Grant GBMF4559](https://www.moore.org/grants/list/GBMF4559), as part of the [Data-Driven Discovery program](https://www.moore.org/programs/science/data-driven-discovery). If you have any questions or want to share some information with the developer / user community, please open a [github issue](https://github.com/stephenslab/dsc2/issues).

## Change Log

### 0.2.x

0.2.6

* Add `--truncate` switch.
* Bring back `--host` option.
* SoS required version 0.9.12.3 for relevant upstream bug fixes.
* [minor] More unit tests and bug fixes.

0.2.5.1

* Change in `seed` behavior: since this release `seed` will no longer be a DSC2 keyword. Users are responsible to set seeds on their own.
* [minor] Allow for both lower case and capitalized operator names `File/file, List/list, Dict/dict`.

0.2.5

* New syntax release, compatible with SoS 0.9.12.1.
* Removed `--host` option due to upstream changes.

### 0.1.x

0.1.0

* First release, compatible with SoS 0.6.4.
