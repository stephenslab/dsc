#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import os, msgpack, yaml, re, glob, pickle
from collections import OrderedDict
from multiprocessing import Process, Manager
import pandas as pd
from sos.utils import Error
from .utils import flatten_list, uniq_list, no_duplicates_constructor, dotdict, chunks, n2a

yaml.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, no_duplicates_constructor)

def remove_obsolete_output(output, additional_files = None, rerun = False):
    from sos.__main__ import cmd_remove
    map_db = '{}/{}.map.mpk'.format(output, os.path.basename(output))
    # Load existing file names
    if os.path.isfile(map_db) and not rerun:
        map_data = msgpack.unpackb(open(map_db, 'rb').read(), encoding = 'utf-8',
                                   object_pairs_hook = OrderedDict)
    else:
        map_data = OrderedDict()
    # Remove file signature when files are deleted
    to_remove = []
    for k, x in list(map_data.items()):
        x = os.path.join(output, x)
        if not (os.path.isfile(x) or os.path.isfile(x + '.zapped')):
            to_remove.append(x)
            del map_data[k]
    # Remove files that are not in the name database
    for x in glob.glob('{}/*'.format(output)):
        if x.endswith(".zapped"):
            x = x[:-7]
            x_ext = '.zapped'
        else:
            x_ext = ''
        if os.path.basename(x) not in map_data.values() and \
           x not in ['{}/{}.{}.mpk'.format(output, os.path.basename(output), i) for i in ['io', 'conf', 'map']]:
            to_remove.append(x + x_ext)
    # Additional files to remove
    for x in additional_files or []:
        if not os.path.isfile(x):
            to_remove.append(x)
    if rerun:
        to_remove = list(glob.glob('.sos/.dsc/*.mpk')) + to_remove
    if len(to_remove):
        open(map_db, "wb").write(msgpack.packb(map_data))
        # Do not limit to tracked or untracked, and do not just remove signature
        cmd_remove(dotdict({"tracked": False, "untracked": False,
                            "targets": to_remove, "external": True,
                            "__confirm__": True, "signature": False,
                            "verbosity": 0, "zap": False,
                            "size": None, "age": None, "dryrun": False}), [])

def load_mpk(mpk_files, jobs):
    d = Manager().dict()
    def f(d, x):
        for xx in x:
            d.update(msgpack.unpackb(open(xx, "rb").read(), encoding = 'utf-8',
                                     object_pairs_hook = OrderedDict))
    #
    mpk_files = [x for x in chunks(mpk_files, int(len(mpk_files) / jobs) + 1)]
    job_pool = [Process(target = f, args = (d, x)) for x in mpk_files]
    for job in job_pool:
        job.start()
    for job in job_pool:
        job.join()
    return OrderedDict([(x, d[x]) for x in sorted(d.keys())])

def build_config_db(input_files, io_db, map_db, conf_db, vanilla = False, jobs = 4):
    '''
    - collect all output file names in md5 style
    - check if map file should be loaded, and load it
    - update map file: remove irrelevant entries; add new file name mapping (starting from max index)
    - create conf file based on map file and io file
    '''
    def get_names():
        '''Get map names. Also dedup data object'''
        # names has to be ordered dict to make sure
        # map_data is updated non-randomly
        # return is a list of original name and new name mapping
        names = OrderedDict()
        lookup = {}
        seen = set()
        base_ids = {}
        # 1. collect exec names and ID
        for k in list(data.keys()):
            # handle duplicate output
            # FIXME: what if there is partial overlap?
            # Maybe I should prevent this via checking input script
            tmp_output = str(sorted(data[k]['DSC_IO_'][1]))
            if tmp_output in seen:
                del data[k]
                continue
            else:
                seen.add(tmp_output)
            for k1 in data[k]:
                if k1 in ["DSC_EXT_", "DSC_IO_"]:
                    continue
                k1 = k1.split()[0]
                # step_key example:
                # [('rcauchy.R', '71c60831e6ac5e824cb845171bd19933'),
                # ('mean.R', 'dfb0dd672bf5d91dd580ac057daa97b9'),
                # ('MSE.R', '0657f03051e0103670c6299f9608e939')]
                step_key = uniq_list(reversed([x for x in chunks(k1.split(":"), 2)]))
                if k1 in map_data:
                    k_tmp = tuple([x[0] for x in step_key])
                    if not k_tmp in base_ids:
                        base_ids[k_tmp] = {x:set() for x in k_tmp}
                    for x, y in zip(k_tmp, step_key):
                        base_ids[k_tmp][x].add(y[1])
                    continue
                if k1 in names:
                    raise ValueError('\nIdentical computational procedures found: ``{}``!'.format(k1))
                names[k1] = step_key
                for x in names[k1]:
                    if x[0] not in lookup:
                        lookup[x[0]] = []
                    if x[1] not in lookup[x[0]]:
                        lookup[x[0]].append(x[1])
                names[k1].append(data[k]["DSC_EXT_"])
        for k in names:
            k_tmp = tuple([x[0] for x in names[k][:-1]])
            base_id = base_ids[k_tmp] if k_tmp in base_ids else {}
            # 2. replace the UUID of executable environment with a unique index
            names[k] = [[x[0], str(lookup[x[0]].index(x[1]) + 1 + \
                                   (len(base_id[x[0]]) if x[0] in base_id and x[1] not in base_id[x[0]] else 0))]
                        for x in names[k][:-1]] + [names[k][-1]]
            # 3. construct name map
            names[k] = '_'.join(flatten_list(names[k][:-1])) + '.{}'.format(names[k][-1])
        return names

    def update_map(names):
        '''Update maps and write to disk'''
        map_data.update(names)
        open(map_db, "wb").write(msgpack.packb(map_data))

    def find_representative(files):
        '''Input files are exec1:id1:exec2:id2:exec3:id3:....rds
        need to return a list of non-random representative files having unique pattern
        '''
        seen = set()
        res = []
        for fn in files:
            keys = tuple([x[0] for x in chunks(fn.split(":"), 2)])
            if keys not in seen:
                res.append(fn)
                seen.add(keys)
        return res

    def find_dependent(conf):
        for sid in conf:
            for name in conf[sid]:
                conf[sid][name]["depends"] = []
                for item in conf[sid][name]['input_repr']:
                    for sid2 in conf:
                        for k in conf[sid2]:
                            if name == k:
                                continue
                            if item in conf[sid2][k]['output_repr']:
                                conf[sid][name]['depends'].append("{}_{}".format(re.sub(r'\d+$', '', k), n2a(int(sid2))))
                if len(conf[sid][name]['depends']) > len(set(conf[sid][name]['depends'])):
                    raise ValueError("Dependent files not unique for sequence {} step {}".format(sid, name))
        for sid in conf:
            for name in conf[sid]:
                del conf[sid][name]['input_repr']
                del conf[sid][name]['output_repr']
        return conf

    #
    if os.path.isfile(map_db) and not vanilla:
        map_data = msgpack.unpackb(open(map_db, 'rb').read(), encoding = 'utf-8',
                                   object_pairs_hook = OrderedDict)
    else:
        map_data = OrderedDict()
    data = load_mpk(input_files, jobs)
    open(io_db, "wb").write(msgpack.packb(data))
    map_names = get_names()
    update_map(map_names)
    fid = os.path.dirname(conf_db)
    conf = OrderedDict()
    for k in data:
        sid, name = k.split(':')
        if sid not in conf:
            conf[sid] = OrderedDict()
        if name not in conf[sid]:
            conf[sid][name] = OrderedDict()
        conf[sid][name]['input'] = [os.path.join(fid, map_data[item]) \
                                    for item in data[k]['DSC_IO_'][0]]
        conf[sid][name]['output'] = [os.path.join(fid, map_data[item]) \
                                     for item in data[k]['DSC_IO_'][1]]
        conf[sid][name]['input_repr'] = [os.path.join(fid, map_data[item]) \
                                         for item in find_representative(data[k]['DSC_IO_'][0])]
        conf[sid][name]['output_repr'] = [os.path.join(fid, map_data[item]) \
                                          for item in find_representative(data[k]['DSC_IO_'][1])]
    conf = find_dependent(conf)
    #
    open(conf_db, "wb").write(msgpack.packb(conf))

class ResultDBError(Error):
    """Raised when there is a problem building the database."""
    def __init__(self, msg):
        Error.__init__(self, msg)
        self.args = (msg, )

class ResultDB:
    def __init__(self, db_prefix, master_names):
        self.db_prefix = db_prefix
        # If this is None, then the last block will be used
        # As master table
        self.master_names = master_names
        # different tables; one exec per table
        self.data = {}
        # master tables
        self.master = {}
        # list of exec names that are the last step in sequence
        self.last_block = []
        # key = block name, item = exec name
        self.groups = {}
        if os.path.isfile(self.db_prefix + '.map.mpk'):
            self.maps = msgpack.unpackb(open(self.db_prefix + '.map.mpk', 'rb').read(), encoding = 'utf-8',
                                        object_pairs_hook = OrderedDict)
        else:
            raise ResultDBError("DSC file name database is corrupted!")

    def load_parameters(self):
        #
        def search_dependent_index(x):
            res = None
            for ii, kk in enumerate(data.keys()):
                if kk.split()[0] == x:
                    res = ii + 1
                    break
            if res is None:
                raise ResultDBError('Cannot find dependency step for output ``{}``!'.format(x))
            return res
        #
        def find_namemap(x):
            if x in self.maps:
                return os.path.splitext(self.maps[x])[0]
            raise ResultDBError('Cannot find name map for ``{}``'.format(x))
        #
        try:
            data_all = msgpack.unpackb(open(self.db_prefix + ".io.mpk", "rb").read(),
                                    encoding = 'utf-8', object_pairs_hook = OrderedDict)
        except:
            raise ResultDBError('Cannot load source data to build database!')
        seen = []
        data = OrderedDict()
        for k0 in data_all.keys():
            for k in list(data_all[k0].keys()):
                if k == 'DSC_IO_' or k == 'DSC_EXT_':
                    continue
                if not k in seen:
                    data[k] = data_all[k0][k]
                    seen.append(k)
        for idx, (k, v) in enumerate(data.items()):
            # each v is a dict
            # collect some meta info
            table = v['exec']
            block_name = v['step_name'].split("_")[:-1]
            block_name = '_'.join(block_name[:-1]) if (block_name[-1].isdigit() and len(block_name) > 1) \
                         else '_'.join(block_name)
            if block_name not in self.groups:
                self.groups[block_name] = []
            if table not in self.groups[block_name]:
                self.groups[block_name].append(table)
            is_last_block = (v['step_name'] == v['sequence_name'].split('+')[-1])
            if self.master_names is not None:
                is_last_block = block_name in self.master_names or is_last_block
            if not block_name in self.last_block and is_last_block:
                self.last_block.append(block_name)
            #
            for x in ['ID', 'FILE', 'parent']:
                if x in v.keys():
                    v['.{}'.format(x)] = v.pop(x)
            #
            if not table in self.data:
                self.data[table] = {}
                for x in list(v.keys()) + ['ID', 'FILE', 'parent']:
                    if x not in ['sequence_id', 'sequence_name', 'step_name', 'exec']:
                        self.data[table][x] = []
            else:
                keys1 = repr(sorted([x for x in v.keys() if not x in
                                     ['sequence_id', 'sequence_name', 'step_name', 'exec']]))
                keys2 = repr(sorted([x for x in self.data[table].keys() if not x in
                                     ['ID', 'FILE', 'parent']]))
                if keys1 != keys2:
                    raise ResultDBError('Inconsistent keys between step '\
                                              '``{0} (value {2})`` and ``{1} (value {3})``.'.\
                                              format(idx + 1, keys1, self.data[table]['ID'], keys2))
            self.data[table]['ID'].append(idx + 1)
            k = k.split()
            self.data[table]['FILE'].append(find_namemap(k[0]))
            if len(k) > 1:
                self.data[table]['parent'].append(search_dependent_index(k[-1]))
            else:
                self.data[table]['parent'].append(-9)
            for k1, v1 in v.items():
                if k1 not in ['sequence_id', 'sequence_name', 'step_name', 'exec']:
                    self.data[table][k1].append(v1)

    def __find_block(self, step):
        for k in self.groups:
            if step in self.groups[k]:
                return k
        raise ResultDBError('Cannot find ``{}`` in any blocks!'.format(step))

    def __get_sequence(self, step, step_id, step_idx, res):
        '''Input are last step name, ID, and corresponding index (in its data frame)'''
        res.append((step, step_id))
        depend_id = self.data[step]['parent'][step_idx]
        if depend_id == -9:
            return
        else:
            idx = None
            step = None
            for k in self.data:
                # try get some idx
                if depend_id in self.data[k]['ID']:
                    idx = self.data[k]['ID'].index(depend_id)
                    step = k
                    break
            if idx is None or step is None:
                raise ResultDBError('Cannot find step_id ``{}`` in any tables!'.format(depend_id))
            self.__get_sequence(step, depend_id, idx, res)

    def write_master_table(self, block):
        '''
        Create a master table in DSCR flavor. Columns are:
        name, block1, block1_ID, block2, block2_ID, ...
        I'll create multiple master tables for as many as last steps.
        Also extend the master table to include information from the
        output of last step
        (step -> id -> depend_id -> step ... )_n
        '''
        res = []
        for step in self.groups[block]:
            for step_idx, step_id in enumerate(self.data[step]['ID']):
                tmp = []
                self.__get_sequence(step, step_id, step_idx, tmp)
                res.append(list(reversed(tmp)))
        data = OrderedDict()
        for item in res:
            key = tuple([self.__find_block(x[0]) for x in item])
            if key not in data:
                data[key] = [flatten_list([('{}_name'.format(x), '{}_id'.format(x)) for x in key])]
            data[key].append(flatten_list(item))
        for key in data:
            header = data[key].pop(0)
            data[key] = pd.DataFrame(data[key], columns = header)
        captain = [x for x in data.keys()]
        data = pd.concat([data[key] for key in data], ignore_index = True)
        id_cols = [k for k in data.keys() if k.endswith("_id")]
        name_cols = [k for k in data.keys() if not k.endswith("_id")]
        data[id_cols] = data[id_cols].fillna(-9, downcast = int)
        data[name_cols] = data[name_cols].fillna("-")
        return data, captain

    def Build(self, script = None):
        self.load_parameters()
        for block in self.last_block:
            self.master['pipeline_{}'.format(block)], \
                self.master['pipeline_{}.captain'.format(block)] = self.write_master_table(block)
        tmp = ['ID', 'parent', 'FILE']
        for table in self.data:
            cols = tmp + [x for x in self.data[table].keys() if x not in tmp]
            self.data[table] = pd.DataFrame(self.data[table], columns = cols)
        self.data.update(self.master)
        if script is not None:
            self.data['.html'] = script
        pickle.dump(self.data, open(self.db_prefix + '.db', 'wb'))
