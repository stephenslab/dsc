#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

from pysos import SoS_Script

class DSCJobs:
    '''
    Convert DSC data to steps compatible with SoS format.
      * Input is DSCData object

    This includes:
      * Ensure step ordering for DSC::run are legitimate
      * Prepare environments to run R: libraries, alias, return alias
      * Prepare environments to run non-R exec: checking / putting together arguments
      * ...

    The output of this will be a DSCJobs object ready to convert to SoS steps
    '''
    def __init__(self, data):
        pass

    def __str__(self):
        return None

    def __call__(self):
        pass

class DSC2SoS:
    '''
    Initialize SoS workflows with DSC jobs
      * Input is DSC job objects
      * Output is SoS workflow codes

    Here are the ideas from DSC to SoS:
      * Each DSC computational routine `exec` is a step; step name is `block name + routine index`
      * When there are combined routines in a block via `.logic` for `exec` then sub-steps are generated
        with name `block name + combined routine index + routine index` index then create nested workflow
        and eventually the nested workflow name will be `block name + combined routine index`
      * Parameters utilize `for_each` and `paired_with`. Of course will have to distinguish input / output
        from parameters (input will be the ones with $ sigil; output will be the ones in return)
      * Parameters might have to be pre-expanded to some degree given limited SoS `for_each` and `paired_with`
        support vs. potentially complicated DSC `.logic`.
      * Final workflow also use nested workflow structure. The number of final workflow is the same as number of
        DSC sequences. These sequences will be executed one after the other
      * Replicates of the first step (assuming simulation) will be sorted out up-front and they will lead to different
        SoS codes.
    '''
    def __init__(self, data):
        pass

    def __call__(self):
        pass
