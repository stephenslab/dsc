
# DSC2

[![PyPI version](https://badge.fury.io/py/sos.svg)](https://badge.fury.io/py/dsc)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/46bb573ea0414f6095f1b7fd4bedbfd3)](https://www.codacy.com/app/gaow/dsc2?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=stephenslab/dsc2&amp;utm_campaign=Badge_Grade)

DSC2 is successor of the [Dynamic Statistical Comparisons in R](https://github.com/stephens999/dscr).

The [project wiki](https://stephenslab.github.io/dsc-wiki) is the main source of documentation for both developers and users of the DSC2 project. If you are new to the concept of DSC, it may worth reading this [blog post](http://stephens999.github.io/blog/2014/10/Data-Driven-Discovery.html) to understand the motivation behind this project.

This work is supported by the the Gordon and Betty Moore Foundation via an Investigator Award to Matthew Stephens, [Grant GBMF4559](https://www.moore.org/grants/list/GBMF4559), as part of the [Data-Driven Discovery program](https://www.moore.org/programs/science/data-driven-discovery). If you have any questions or want to share some information with the developer / user community, please open a [github issue](https://github.com/stephenslab/dsc2/issues).

## Change Log

### 0.2.x

0.2.6.1

* Require `PBS` type of queue to have a corresponding headnode configuration (for customized `job_template`)
* SoS required version bumped to 0.9.12.6 for the new feature above.
* [minor] Various bug fixes.

0.2.6

* Bring back `--host` option; add a companion option `--to-host` to facilicate sending resources to remote computer.
* Add `--truncate` switch.
* SoS required version bumped to 0.9.12.3 for relevant upstream bug fixes.
* [minor] Improved command interface.

0.2.5.2

* SoS required version bumped to 0.9.12.2 for relevant upstream bug fixes.

0.2.5.1

* Change in `seed` behavior: since this release `seed` will no longer be a DSC2 keyword. Users are responsible to set seeds on their own.
* [minor] Allow for both lower case and capitalized operator names `File/file, List/list, Dict/dict`.

0.2.5

* New syntax release, compatible with SoS 0.9.12.1.
* Removed `--host` option due to upstream changes.

### 0.1.x

0.1.0

* First release, compatible with SoS 0.6.4.
