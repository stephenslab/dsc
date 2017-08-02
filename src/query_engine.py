#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import sys, os, msgpack, yaml, re, glob, pickle
from collections import OrderedDict
import pandas as pd
from .dsc_parser import DSC_Script
from .dsc_database import ResultDBError
from .utils import load_rds, save_rds, \
     flatten_list, uniq_list, no_duplicates_constructor, \
     cartesian_list, extend_dict, strip_dict, \
     try_get_value

yaml.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, no_duplicates_constructor)

class ResultAnnotator:
    def __init__(self, ann_file, ann_table, output = None, sequence = None):
        '''Load master table to be annotated and annotation contents'''
        ann = yaml.load(open(ann_file))
        if ann is None:
            raise ResultDBError("Annotation file ``{}`` does not contain proper annotation information!".\
                                 format(ann_file))
        # Annotation groups
        if try_get_value(ann, ('DSC', 'configuration')) is None:
            raise ResultDBError("Cannot find required entry ``DSC::configuration`` in ``{}``".\
                                format(ann_file))
        self.dsc = DSC_Script(ann['DSC']['configuration'], output = output, sequence = sequence)
        if 'groups' in ann['DSC']:
            self.groups = ann['DSC']['groups']
        else:
            self.groups = OrderedDict()
        pattern = re.compile("^\s+|\s*,\s*|\s+$")
        for k, value in self.groups.items():
            self.groups[k] = pattern.split(value)
        for item in flatten_list(self.groups.values()):
            if item and item not in ann:
                raise ResultDBError("Cannot find group ``{}`` in any annotation tags.".format(item))
        del ann['DSC']
        if len(ann) == 0:
            ann = self.dsc.AddDefaultAnnotation()
        #
        data = pickle.load(open('.sos/.dsc/{}.db'.format(os.path.basename(self.dsc.runtime.output)), 'rb'))
        self.data = {k : pd.DataFrame(v) for k, v in data.items() if k != '.dscsrc'}
        if ann_table is not None:
            self.masters = [ann_table if ann_table.startswith('master_') else 'master_{}'.format(ann_table)]
        else:
            self.masters = [k for k in self.data if k.startswith('master_')]
        for master in self.masters:
            if master not in data:
                raise ValueError('Cannot find target block ``{}``.'.format(master[7:]))
        #
        if not isinstance(ann, list):
            ann = [ann]
        self.anns = ann

    def Apply(self, master):
        self.master = master
        self.msg = []
        self.result = OrderedDict()
        for ann in self.anns:
            queries = self.ConvertAnnToQuery(ann)
            result = self.ApplyAnnotation(queries)
            for key in result:
                if not key in self.result:
                    self.result[key] = {}
                self.result[key] = extend_dict(self.result[key], result[key], unique = True)
        tagfile, shinyfile = self.SaveAnnotation()
        self.msg = uniq_list(self.msg)
        return tagfile, shinyfile

    def ConvertAnnToQuery(self, ann):
        '''
        Parse annotations to make pytable syntax
        1. for simple numbers / strings use '==' logic
        2. for list / tuples use "or" logic
        3. for raw queries use a special function ... maybe starting with % sign?
        4. non-master table queries: if `exec` presents use the tables specified and double check the table names; otherwise do it for all the tables in the given block.
        '''
        def get_query(obj, text):
            def to_str(p1):
                p1 = p1.strip()
                try:
                    res = re.search(r'^Asis\((.*?)\)$', p1).group(1)
                    return repr(res)
                except Exception as e:
                    return repr(repr(p1))

            if isinstance(text, str) and text.startswith('%'):
                return text.lstrip('%')
            else:
                if isinstance(text, list) or isinstance(text, tuple):
                    return ' OR '.join(['{} == {}'.format(obj, x if not isinstance(x, str) else to_str(x)) for x in text])
                else:
                    return '{} == {}'.format(obj, text if not isinstance(text, str) else to_str(text))

        queries = OrderedDict()
        for tag in ann:
            queries[tag] = []
            # for each tag we need a query
            for block in ann[tag]:
                # get subtables
                if 'exec' in ann[tag][block]:
                    # we potentially found the sub table to query from
                    subtables = [os.path.splitext(os.path.basename(x))[0] for x in ann[tag][block]['exec'].split(',')]
                else:
                    # we take that all sub tables in this block is involved
                    # and we look for these tables in DSC data
                    subtables = [x.name for x in self.dsc.blocks[block].steps]
                # get query
                block_query = []
                for k1 in ann[tag][block]:
                    if k1 == 'params':
                        for k2 in ann[tag][block][k1]:
                            block_query.append(get_query(k2, ann[tag][block][k1][k2]))
                    elif k1 == 'exec':
                        continue
                    else:
                        block_query.append(get_query(k1, ann[tag][block][k1]))
                # OR logic for multiple subtables
                queries[tag].append(['[{}] {}'.format(table, ' AND '.join(['({})'.format(x) for x in block_query]) if len(block_query) else 'ALL') for table in subtables])
        for tag in queries:
            queries[tag] = cartesian_list(*queries[tag])
        return queries

    def ApplyAnnotation(self, queries):
        '''Run query on result table and make a tag column'''

        def get_id(query, target = None):
            name = self.master[7:] if self.master.startswith('master_') else self.master
            query = query.strip()
            if target is None:
                col_id = self.data[self.master].query(query)[name + '_id'] if query != 'ALL' else self.data[self.master][name + '_id']
                col_id = col_id.tolist()
            else:
                col_id = self.data[target[0]].query(query)['step_id'] if query != 'ALL' else self.data[target[0]]['step_id']
                col_id = [x for x, y in zip(self.data[self.master][name + '_id'].tolist(),
                                            self.data[self.master][target[1][:-5] + '_id'].\
                                            isin(col_id).tolist()) if y]
            return col_id
        #
        def get_output(name, col_id):
            # Get list of file names
            # given name of column and target col_id's
            lookup = {}
            for x, y in zip(self.data[self.master].query('{}_id == @col_id'.format(name))[name + '_name'].tolist(), col_id):
                if x not in lookup:
                    lookup[x] = []
                lookup[x].append(y)
            res = []
            for k, value in lookup.items():
                # FIXME: cannot use `.query('step_id == @value')`
                # because it cannot propagate duplicate values
                # which is what we want to do here
                # implementation below maybe inefficient
                # can be improved in the future
                step_id_list = self.data[k]['step_id'].tolist()
                rows = [step_id_list.index(x) for x in value]
                res.append(self.data[k].iloc[rows][['return']])
            res = pd.concat(res)
            for item in ['{}_id'.format(name), 'step_id']:
                if item in res.columns.values:
                    res.drop(item, axis = 1, inplace = True)
            return res
        #
        def run_query(text):
            return_id = None
            # Get ID for the last step as result of the query
            for item in text:
                pattern = re.search(r'^\[(.*)\](.*)', item)
                if pattern:
                    # query from sub-table
                    for k in self.data[self.master]:
                        if pattern.group(1) in self.data[self.master][k].tolist():
                            if return_id is None:
                                return_id = get_id(pattern.group(2).strip(), (pattern.group(1).strip(), k))
                            else:
                                return_id = [x for x in get_id(pattern.group(2).strip(),
                                                               (pattern.group(1).strip(), k))
                                             if x in return_id]
                            break
                        else:
                            continue
                else:
                    # query from master table
                    if return_id is None:
                        return_id = get_id(item)
                    else:
                        return_id = [x for x in get_id(item) if x in return_id]
            if return_id is None or len(return_id) == 0:
                self.msg.append("Cannot find matching entries based on query ``{}``".format(repr(text)))
                res = {}
            else:
                res = {k: [] for k in [x[:-3] for x in self.data[self.master].keys() if x.endswith('_id')]}
                for k in res:
                    if k == self.master[7:]:
                        res[k] = get_output(k, return_id)['return'].tolist()
                    else:
                        target_id = self.data[self.master].loc[
                            self.data[self.master]['{}_id'.format(self.master[7:])].isin(return_id)]['{}_id'.format(k)].\
                            dropna()
                        if target_id.size:
                            res[k] = get_output(k, target_id)['return'].tolist()
            return res
        #
        result = OrderedDict()
        for tag in queries:
            result[tag] = OrderedDict()
            for item in queries[tag]:
                result[tag] = extend_dict(result[tag], run_query(item))
        return strip_dict(result, mapping = OrderedDict)

    def ShowQueries(self):
        '''Make a table summary of what has been performed'''
        from prettytable import PrettyTable
        res = PrettyTable()
        res.field_names = ["Tag", "No. unique obj."]
        for tag in self.result:
            counts = ['``{}`` {}'.format(len(set(self.result[tag][block])), block) for block in sorted(self.result[tag])]
            res.add_row(["``{}``".format(tag), ' & '.join(counts)])
        res.align = "l"
        return res.get_string(padding_width = 2)

    def SaveAnnotation(self):
        '''Save some meta info for shinydsc and for --extract commands'''
        # Get available var menu
        var_menu = []
        lask_blocks = [k[7:] for k in self.data if k.startswith('master_')]
        for block in self.dsc.blocks:
            if block in lask_blocks and block != self.master[7:]:
                continue
            # FIXME: Here assuming all steps have the same output variables
            for item in set(flatten_list([list(step.rv.keys()) for step in self.dsc.blocks[block].steps])):
                var_menu.append('{}:{}'.format(block, item))
        res = {'tags': sorted(flatten_list([list(ann.keys()) for ann in self.anns])),
                              'variables': var_menu, 'groups': self.groups}
        metafile = os.path.join('.sos/.dsc', self.dsc.runtime.output + '.{}.shinymeta.rds'.format(self.master[7:]))
        save_rds(res, metafile)
        # save tag
        tagfile = os.path.join('.sos/.dsc', self.dsc.runtime.output + '.{}.tags'.format(self.master[7:]))
        open(tagfile, "wb").write(msgpack.packb(self.result))
        return metafile, tagfile


EXTRACT_RDS_R = '''
res = list()
res$DSC_TIMER = list()
keys = c(${key!r,})
for (key in keys) {
  res[[key]] = list()
  res$DSC_TIMER[[key]] = list()
}
targets = c(${target!r,})
f_counter = 1
for (item in c(${input!r,})) {
  tryCatch({
    dat = readRDS(item)
    for (idx in length(targets)) {
       res[[keys[idx]]][[f_counter]] = dat[[targets[idx]]]
       res$DSC_TIMER[[keys[idx]]][[f_counter]] = dat$DSC_TIMER[1]
    }
  }, error = function(e) {})
  for (idx in length(targets)) {
    if (length(res[[keys[idx]]]) < f_counter) {
      res[[keys[idx]]][[f_counter]] = item
      res$DSC_TIMER[[keys[idx]]][[f_counter]] = NA
    }
  }
  f_counter = f_counter + 1
}
saveRDS(res, ${output!r})
'''

CONCAT_RDS_R = '''
res = list()
for (dat in lapply(c(${input!r,}), readRDS)) {
  for (item in names(dat)) {
    if (item != 'DSC_TIMER') {
      res[[item]] = dat[[item]]
    } else {
      for (ii in names(dat[[item]])) {
        res[[item]][[ii]] = unlist(dat[[item]][[ii]])
      }
    }
  }
}
res$DSC_COMMAND = ${command!r}
saveRDS(res, ${output!r})
'''


class ResultExtractor:
    def __init__(self, project_name, tags, from_table, to_file, targets_list):
        tag_file = glob.glob('.sos/.dsc/{}.*.tags'.format(project_name))
        tables = [x.split('.')[-2] for x in tag_file]
        if len(tag_file) == 0:
                raise ValueError("DSC result has not been annotated. Please use ``-a`` option to annotate the results before running ``-e``.")
        if from_table is not None and not from_table in tables:
                raise ValueError("DSC result for ``{}`` has not been annotated. Please use ``-a`` option to annotate the results before running ``-e``.".format(from_table))
        if len(tag_file) == 1:
            # we have a unique table to extract from
            self.master = tag_file[0].split('.')[-2]
        else:
            if from_table:
                self.master = from_table
            else:
                raise ValueError("Please specify the DSC block to target, via ``--target``."\
                                 "\nChoices are ``{}``".\
                                 format(repr([x.split('.')[-2] for x in tag_file])))
        tag_file = tag_file[tables.index(self.master)]
        self.ann = msgpack.unpackb(open(tag_file, 'rb').read(), encoding = 'utf-8',
                                   object_pairs_hook = OrderedDict)
        valid_vars = load_rds(tag_file[:-4] + 'shinymeta.rds')['variables'].tolist()
        if tags is None:
            self.tags = {x:x for x in self.ann.keys()}
        else:
            self.tags = {}
            for tag in tags:
                tag = tag.strip().strip('=') # add this line in case shinydsc gives empty tag alias
                if "=" in tag:
                    if len(tag.split('=')) != 2:
                        raise ValueError("Invalid tag syntax ``{}``!".format(tag))
                    self.tags[tag.split('=')[0].strip()] = tag.split('=')[1].strip()
                else:
                    self.tags['_'.join([x.strip() for x in tag.split('&&')])] = tag
        self.name = os.path.split(tag_file)[1].rsplit('.', 2)[0]
        if to_file is None:
            to_file = self.name + '.{}.rds'.format(self.master)
        self.output = to_file
        self.ann_cache = []
        self.script = []
        # Organize targets
        targets = {}
        for item in targets_list:
            if not item in valid_vars:
                raise ValueError('Invalid input value: ``{}``. \nChoices are ``{}``.'.\
                                 format(item, repr(valid_vars)))
            target = item.split(":")
            if target[0] not in targets:
                targets[target[0]] = []
            targets[target[0]].append(target[1])
        # Compose executable job file
        for key, item in targets.items():
            for tag, ann in self.tags.items():
                input_files = []
                # Handle union logic
                if not '&&' in ann and ann in self.ann and key in self.ann[ann]:
                    input_files = sorted(self.ann[ann][key])
                else:
                    arrays = [self.ann[x.strip()][key] for x in ann.split('&&')
                              if x.strip() in self.ann and key in self.ann[x.strip()]]
                    input_files = sorted(set.intersection(*map(set, arrays)))
                if len(input_files) == 0:
                    continue
                input_files = flatten_list([glob.glob("{}/{}.*".format(self.name, x)) for x in input_files])
                output_prefix = ['_'.join([tag, key, x]) for x in item]
                step_name = '{}_{}'.format(tag, key)
                output_file = '{}/ann_cache/{}.rds'.format(self.name, step_name)
                # Compose execution step
                self.ann_cache.append(output_file)
                self.script.append("[{0}: provides = '{1}']".\
                                   format(step_name, output_file))
                self.script.append('parameter: target = {}'.format(repr(item)))
                self.script.append('parameter: key = {}'.format(repr(output_prefix)))
                self.script.append('input: [{}]'.format(','.join([repr(x) for x in input_files])))
                self.script.append('output: \'{}\''.format(output_file))
                self.script.extend(['R:', EXTRACT_RDS_R])
        self.script.append("[Extracting (concatenate RDS)]")
        self.script.append('parameter: command = "{}"'.format(' '.join(sys.argv)))
        self.script.append('depends: [{}]'.format(','.join([repr(x) for x in sorted(self.ann_cache)])))
        self.script.append('input: [{}]'.format(','.join([repr(x) for x in sorted(self.ann_cache)])))
        self.script.append('output: {}'.format(repr(self.output)))
        self.script.extend(['R:', CONCAT_RDS_R])
        self.script = '\n'.join(self.script)
