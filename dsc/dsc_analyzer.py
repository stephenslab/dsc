#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
This file analyzes DSC contents to determine dependencies
and what exactly should be executed.
'''

import copy
from .utils import FormatError, OrderedDict

__all__ = ['DSC_Analyzer']

class DSC_Analyzer:
    '''Putting together DSC steps to sequences and continue to propagate computational routines for execution
    * Handle step dependencies
    * Merge steps: via block rule
    * Merge blocks: via block option (NOT IMPLEMENTED)
    The output will be analyzed DSC ready to be translated to pipelines for execution
    '''
    def __init__(self, script_data):
        '''
        script_data comes from DSC_Script, having members:
        * blocks
        * runtime (runtime.sequence is relevant)
        * output (output data prefix)
        Because different combinations of blocks will lead to different
        I/O settings particularly with plugin status, here for each
        sequence, fresh deep copies of blocks will be made from input blocks

        Every step in a workflow is a plugin, whether it be Python, R or Shell.
        When output contain variables the default RDS format and method to save variables are
        applied.

        This class provides a member called `workflow` which contains multiple workflows 
        '''
        self.workflows = []
        for sequence in script_data.runtime.sequence:
            self.add_workflow(sequence[0], script_data.blocks, list(script_data.runtime.sequence_ordering.keys()))
        self.consolidate_workflows()

    def add_workflow(self, sequence, data, ordering):
        workflow = OrderedDict()
        for name in sequence:
            block = copy.deepcopy(data[name])
            for idx, step in enumerate(block.steps):
                for k, p in step.p.items():
                    for p1 in p:
                        if isinstance(p1, str):
                            if p1.startswith('$'):
                                dependencies = self.find_dependencies(p1[1:], list(workflow.values()))
                                for item in dependencies:
                                    if item not in block.steps[idx].depends:
                                        block.steps[idx].depends.append(item)
                                block.steps[idx].plugin.add_input(k, p1)
                block.steps[idx].depends.sort(key = lambda x: ordering.index(x[0]))
                block.steps[idx].exe_id = idx + 1
            workflow[block.name] = block
        self.workflows.append(workflow)

    def find_dependencies(self, variable, workflow):
        curr_idx = len(workflow)
        if curr_idx == 0:
            raise FormatError('Symbol ``$`` is not allowed in the first step of a DSC sequence.')
        curr_idx = curr_idx - 1
        dependencies = []
        while curr_idx >= 0:
            # Look up backwards for the corresponding block, looking at the output of the first step
            if variable in [x for x in workflow[curr_idx].steps[0].rv]:
                dependencies.append((workflow[curr_idx].name, variable, 'var'))
            if variable in [x for x in workflow[curr_idx].steps[0].rf]:
                dependencies.append((workflow[curr_idx].name, variable, 'file'))
            if len(dependencies):
                break
            else:
                curr_idx = curr_idx - 1
        if len(dependencies) == 0:
            raise FormatError('Cannot find return variable for ``${}`` in any of its previous steps.'.\
                              format(variable))
        return dependencies

    def consolidate_workflows(self):
        '''
        For trivial multiple workflows, eg, "step1 * step2[1], step1 * step[2]", should be consolidated to one
        This cannot be done with symbolic math logic so we have to do it here

        First we compare each workflow (a list of OrderedDict) and get rid of duplicate
        Second ... is there a second?
        '''
        import hashlib
        import pickle
        def get_md5(data):
            return hashlib.md5(pickle.dumps(data)).hexdigest()
        workflows = []
        ids = []
        for workflow in self.workflows:
            md5 = get_md5(workflow)
            if md5 not in ids:
                ids.append(md5)
                workflows.append(workflow)
        self.workflows = workflows

    def __str__(self):
        res = ''
        for idx, blocks in enumerate(self.workflows):
            res += '# Workflow {}\n'.format(idx + 1)
            res += '## Blocks\n' + '\n'.join(['### {}\n```yaml\n{}\n```\n'.format(x, y) for x, y in blocks.items()])
        return res
