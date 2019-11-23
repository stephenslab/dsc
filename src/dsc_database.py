#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import os, msgpack, glob, pickle, copy, shutil
import pandas as pd
from collections import OrderedDict
from .utils import uniq_list, flatten_list, chunks, remove_multiple_strings, extend_dict, \
    remove_quotes, DBError
from .addict import Dict as dotdict
from .syntax import DSC_CACHE

def remove_obsolete_output(output, additional_files = None, rerun = False):
    from sos.__main__ import cmd_remove
    map_db = f'{output}/{os.path.basename(output)}.map.mpk'
    # Load existing file names
    if os.path.isfile(map_db) and not rerun:
        map_data = msgpack.unpackb(open(map_db, 'rb').read(), encoding = 'utf-8',
                                   object_pairs_hook = OrderedDict)
    else:
        map_data = OrderedDict()
    # Remove file signature when files are deleted
    to_remove = []
    for k, x in list(map_data.items()):
        if k == '__base_ids__':
            continue
        x = os.path.join(output, x)
        if not (os.path.isfile(x) or os.path.isfile(x + '.zapped')):
            to_remove.append(x)
            del map_data[k]
    # Remove files that are not in the name database
    for x in glob.glob(f'{output}/**/*.*', recursive = True):
        if x.endswith(".zapped"):
            x = x[:-7]
            x_ext = '.zapped'
        else:
            x_ext = ''
        x_name =  os.path.join(os.path.basename(os.path.split(x)[0]), os.path.basename(x))
        if x_name not in map_data.values() and \
           x not in [f'{output}/{os.path.basename(output)}.{i}.mpk' for i in ['conf', 'map']] and \
               x != f'{output}/{os.path.basename(output)}.db':
            to_remove.append(x + x_ext)
    # Additional files to remove
    for x in additional_files or []:
        if not os.path.isfile(x):
            to_remove.append(x)
    if rerun:
        to_remove = list(glob.glob(f'{DSC_CACHE}/{os.path.basename(output)}_*.mpk')) + to_remove
    if len(to_remove):
        open(map_db, "wb").write(msgpack.packb(map_data))
        # Do not limit to tracked or untracked, and do not just remove signature
        cmd_remove(dotdict({"tracked": False, "untracked": False,
                            "targets": to_remove, "external": True,
                            "__confirm__": True, "signature": False,
                            "verbosity": 0, "zap": False,
                            "size": None, "age": None, "dryrun": False}), [])
    else:
        print("Nothing found to remove!")

def remove_unwanted_output(workflows, groups, modules, db, zap=False):
    from sos.__main__ import cmd_remove
    filename = f'{db}/{os.path.basename(db)}.db'
    to_remove = [x for x in modules if os.path.isfile(x)]
    modules = [x for x in modules if x not in to_remove]
    modules = uniq_list(flatten_list([x if x not in groups else groups[x] for x in modules]))
    remove_modules = []
    if not os.path.isfile(filename):
        raise ValueError(f'Cannot remove ``{repr(modules)}``, due to missing output database ``{filename}``.')
    else:
        for module in modules:
            removed = False
            for workflow in workflows:
                if module in workflow:
                    remove_modules.append(module)
                    removed = True
                    break
            if removed:
                remove_modules.append(module)
            else:
                print(f"Target \"{module}\" ignored because it is not module in current DSC.")
        #
    if (len(to_remove) or len(remove_modules)) and not zap:
        for item in remove_modules:
            shutil.rmtree(f"{db}/{item}", ignore_errors=True)
        for item in to_remove:
            os.remove(item)
    elif zap:
        data = pickle.load(open(filename, 'rb'))
        to_remove.extend(flatten_list([[glob.glob(os.path.join(db, f'{x}.*'))
                                        for x in data[item]['__output__']]
                                       for item in remove_modules if item in data]))
        if len(to_remove) and not \
           (all([True if x.endswith('.zapped') and not x.endswith('.zapped.zapped') else False
                         for x in to_remove])):
            cmd_remove(dotdict({"tracked": False, "untracked": False,
                            "targets": uniq_list(to_remove), "external": True,
                            "__confirm__": True, "signature": False,
                            "verbosity": 0, "zap": True,
                            "size": None, "age": None, "dryrun": False}), [])
        else:
            print("Nothing found to replace!")
    else:
        print("Nothing found to remove!")

def build_config_db(io_db, map_db, conf_db, vanilla = False, jobs = 4):
    '''
    - collect all output file names in md5 style
    - check if map file should be loaded, and load it
    - update map file: remove irrelevant entries; add new file name mapping (starting from max index)
    - create conf file based on map file and io file
    '''
    def get_names():
        '''Get map names.'''
        # names has to be ordered dict to make sure
        # map_data is updated non-randomly
        # return is a list of original name and new name mapping
        names = OrderedDict()
        lookup = dict()
        base_ids = map_data['__base_ids__'] if '__base_ids__' in map_data else dict()
        # 1. collect sequence names and hash
        for k in list(data.keys()):
            for kk in data[k]:
                if kk in ["__ext__", "__input_output___"]:
                    continue
                kk = kk.split(' ')[0]
                if kk in names:
                    raise ValueError(f'\nIdentical instances found in module ``{kk.split(":")[0]}``!')
                content = uniq_list(reversed([x for x in chunks(kk.split(":"), 2)]))
                # content example:
                # [('rcauchy', '71c60831e6ac5e824cb845171bd19933'),
                # ('mean', 'dfb0dd672bf5d91dd580ac057daa97b9'),
                # ('MSE', '0657f03051e0103670c6299f9608e939')]
                k_core = tuple([x[0] for x in content])
                key = ':'.join(k_core)
                if key not in base_ids:
                    base_ids[key] = dict([(x, 0) for x in k_core])
                if key not in lookup:
                    lookup[key] = dict()
                if kk in map_data:
                    # same module signature already exist
                    # will not work on the name map of these
                    # but will have to find their max ids
                    # so that we know how to properly name new comers
                    # ie we count how many times each of the module has occured
                    # in this particular sequence
                    ids = os.path.splitext(remove_multiple_strings(map_data[kk], kk.split(':')[::2]))[0]
                    ids = [int(s) for s in ids.split('_') if s.isdigit()]
                    for i, x in enumerate(base_ids[key].keys()):
                        base_ids[key][x] = max(base_ids[key][x], ids[i])
                    names[kk] = map_data[kk]
                else:
                    lookup[key] = extend_dict(lookup[key], dict(content), unique = True)
                    names[kk] = content
                    names[kk].append(data[k]["__ext__"])
        new_base_ids = copy.deepcopy(base_ids)
        for k in names:
            # existing items in map_data, skip them
            if isinstance(names[k], str):
                continue
            # new items to be processed
            k_core = dict(names[k][:-1])
            key = ':'.join(tuple(k_core.keys()))
            # 2. replace the hash with an ID
            new_name = []
            for kk in k_core:
                new_id = base_ids[key][kk] + lookup[key][kk].index(k_core[kk]) + 1
                new_name.append(f'{kk}_{new_id}')
                new_base_ids[key][kk] = max(new_base_ids[key][kk], new_id)
            # 3. construct name map
            names[k] = f'{k.split(":", 1)[0]}/' + '_'.join(new_name) + f'.{names[k][-1]}'
        names['__base_ids__'] = new_base_ids
        return names

    def update_map(names):
        '''Update maps and write to disk'''
        map_data.update(names)
        open(map_db, "wb").write(msgpack.packb(map_data))

    #
    if os.path.isfile(map_db) and not vanilla:
        map_data = msgpack.unpackb(open(map_db, 'rb').read(), encoding = 'utf-8',
                                   object_pairs_hook = OrderedDict)
    else:
        map_data = OrderedDict()
    data = msgpack.unpackb(open(io_db, 'rb').read(), encoding = 'utf-8',
                           object_pairs_hook = OrderedDict)
    meta_data = msgpack.unpackb(open(io_db[:-4] + '.meta.mpk', 'rb').read(), encoding = 'utf-8',
                                object_pairs_hook = OrderedDict)
    map_names = get_names()
    update_map(map_names)
    fid = os.path.dirname(str(conf_db))
    conf = OrderedDict()
    for key in meta_data:
        workflow_id = str(key)
        if workflow_id not in conf:
            conf[workflow_id] = OrderedDict()
        for module in meta_data[key]:
            k = f'{module}:{key}'
            if k not in data:
                k = f'{meta_data[key][module][0]}:{meta_data[key][module][1]}'
                # FIXME: this will be a bug if ever triggered
                if k not in data:
                    raise DBError(f"Cannot find key ``{k}`` in DSC I/O records.")
                conf[workflow_id][module] = (str(meta_data[key][module][1]), meta_data[key][module][0])
                continue
            if module not in conf[workflow_id]:
                conf[workflow_id][module] = OrderedDict()
            conf[workflow_id][module]['input'] = [os.path.join(fid, map_data[item]) \
                                        for item in data[k]['__input_output___'][0]]
            conf[workflow_id][module]['output'] = [os.path.join(fid, map_data[item]) \
                                         for item in data[k]['__input_output___'][1]]
            # eg. ['normal:a9f57519', 'median:98b37c9a:normal:a9f57519']
            depends_steps = uniq_list([x.split(':')[0] for x in data[k]['__input_output___'][0]])
            conf[workflow_id][module]['depends'] = [meta_data[key][x] for x in depends_steps]
    #
    open(conf_db, "wb").write(msgpack.packb(conf))

class ResultDB:
    def __init__(self, prefix):
        self.prefix = prefix
        # data: every module is a table
        self.data = OrderedDict()
        if os.path.isfile(f"{self.prefix}.map.mpk"):
            self.maps = msgpack.unpackb(open(f"{self.prefix}.map.mpk", "rb").read(), encoding = 'utf-8',
                                        object_pairs_hook = OrderedDict)
        else:
            raise DBError(f"Cannot build DSC meta-data: hash table ``{self.prefix}.map.mpk`` is missing!")
        self.meta_kws = ['__id__', '__output__', '__parent__', '__out_vars__']

    def load_parameters(self):
        #
        def find_namemap(x):
            if x in self.maps:
                return self.maps[x][:-len_ext]
            raise DBError('Cannot find name map for ``{}``'.format(x))
        #
        try:
            self.rawdata = msgpack.unpackb(open(f'{DSC_CACHE}/{os.path.basename(self.prefix)}.io.mpk', 'rb').read(),
                                                encoding = 'utf-8', object_pairs_hook = OrderedDict)
            self.metadata = msgpack.unpackb(open(f'{DSC_CACHE}/{os.path.basename(self.prefix)}.io.meta.mpk', 'rb').read(),
                                                encoding = 'utf-8', object_pairs_hook = OrderedDict)
        except:
            raise DBError('Cannot load source data to build database!')
        KWS = ['__pipeline_id__', '__pipeline_name__', '__module__', '__out_vars__']
        seen = set()
        for workflow in self.metadata.values():
            for module in list(workflow.keys()):
                pipeline_module = f"{workflow[module][0]}:{workflow[module][1]}"
                if pipeline_module in seen:
                    continue
                seen.add(pipeline_module)
                data = self.rawdata[pipeline_module]
                #
                len_ext = len(data['__ext__']) + 1
                for k, v in data.items():
                    if k in ['__input_output___', '__ext__']:
                        continue
                    if module not in self.data:
                        self.data[module] = dict([(x, []) for x in self.meta_kws])
                        self.data[module]['__out_vars__'] = v['__out_vars__']
                    # each v is a dict of a module instances
                    # each key reads like
                    # "shrink:a8bd873083994102:simulate:bd4946c8e9f6dcb6 simulate:bd4946c8e9f6dcb6"
                    k = k.split(' ')
                    # ID numbers all module instances
                    num_parents = 1
                    if len(k) > 1:
                        # Have to fine its ID ...
                        # see which module has __module_id__ == k[i] and return its ID
                        num_parents = len(k[1:])
                        self.data[module]['__parent__'].extend(k[1:])
                    else:
                        self.data[module]['__parent__'].append(None)
                    # Assign other parameters
                    self.data[module]['__output__'].extend([find_namemap(k[0])] * num_parents)
                    self.data[module]['__id__'].extend([k[0]] * num_parents)
                    for kk, vv in v.items():
                        if kk not in KWS:
                            if kk not in self.data[module]:
                                self.data[module][kk] = []
                            self.data[module][kk].extend([remove_quotes(vv)] * num_parents)

    def Build(self, script = None, groups = None, depends = None, pipelines = None):
        self.load_parameters()
        output = dict()
        for module in self.data:
            cols = ['__id__', '__parent__', '__output__'] + [x for x in self.data[module].keys() if x not in self.meta_kws]
            output[module] = self.data[module].pop('__out_vars__')
            self.data[module] = pd.DataFrame(self.data[module], columns = cols)
        if script is not None:
            self.data['.html'] = script
        if groups is not None:
            self.data['.groups'] = groups
        if depends is not None:
            self.data['.depends'] = depends
        self.data['.output'] = output
        self.data['.pipelines'] = pipelines
        pickle.dump(self.data, open(self.prefix + '.db', 'wb'))

if __name__ == '__main__':
    import sys
    ResultDB(sys.argv[1], sys.argv[2], None).Build('NULL')