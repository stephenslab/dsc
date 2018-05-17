#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

'''
Test rpy2 installation:
python -m 'rpy2.tests'
'''

def load_mpk(mpk_files, jobs = 2):
    import msgpack, collections
    from multiprocessing import Process, Manager
    from .utils import chunks
    if isinstance(mpk_files, str):
        return msgpack.unpackb(open(mpk_files, "rb").read(), encoding = 'utf-8',
                                     object_pairs_hook = collections.OrderedDict)
    d = Manager().dict()
    def f(d, x):
        for xx in x:
            d.update(msgpack.unpackb(open(xx, "rb").read(), encoding = 'utf-8',
                                     object_pairs_hook = collections.OrderedDict))
    #
    mpk_files = [x for x in chunks(mpk_files, int(len(mpk_files) / jobs) + 1)]
    job_pool = [Process(target = f, args = (d, x)) for x in mpk_files]
    for job in job_pool:
        job.start()
    for job in job_pool:
        job.join()
    return collections.OrderedDict([(x, d[x]) for x in sorted(d.keys(), key = lambda x: int(x.split(':')[0]))])

def load_rds(filename, types = None):
    import os
    import pandas as pd, numpy as np
    import rpy2.robjects as RO
    import rpy2.robjects.vectors as RV
    import rpy2.rinterface as RI
    from rpy2.robjects import numpy2ri
    numpy2ri.activate()
    from rpy2.robjects import pandas2ri
    pandas2ri.activate()
    def load(data, types):
         if types is not None and not isinstance(data, types):
              return np.array([])
         if isinstance(data, RI.RNULLType):
              res = None
         elif isinstance(data, RV.BoolVector):
              data = RO.r['as.integer'](data)
              res = np.array(data, dtype = int)
              # Handle c(NA, NA) situation
              if np.sum(np.logical_and(res != 0, res != 1)):
                   res = res.astype(float)
                   res[res < 0] = np.nan
                   res[res > 1] = np.nan
         elif isinstance(data, RV.FactorVector):
              data = RO.r['as.character'](data)
              res = np.array(data, dtype = str)
         elif isinstance(data, RV.IntVector):
              res = np.array(data, dtype = int)
         elif isinstance(data, RV.FloatVector):
              res = np.array(data, dtype = float)
         elif isinstance(data, RV.StrVector):
              res = np.array(data, dtype = str)
         elif isinstance(data, RV.DataFrame):
              res = pd.DataFrame(data)
         elif isinstance(data, RV.Matrix):
              res = np.matrix(data)
         elif isinstance(data, RV.Array):
              res = np.array(data)
         elif isinstance(data, RI.NULL):
              res = None
         else:
              # I do not know what to do for this
              # But I do not want to throw an error either
              res = str(data)
         if isinstance(res, np.ndarray) and res.shape == (1,):
             res = res[0]
         return res

    def load_dict(res, data, types):
        '''load data to res'''
        names = data.names if data.names else [i + 1 for i in range(len(data))]
        for name, value in zip(names, list(data)):
            if isinstance(value, RV.ListVector):
                res[name] = {}
                res[name] = load_dict(res[name], value, types)
            else:
                res[name] = load(value, types)
        return res
    #
    if not os.path.isfile(filename):
        raise IOError('Cannot find file ``{}``!'.format(filename))
    rds = RO.r['readRDS'](filename)
    if isinstance(rds, RV.ListVector):
        res = load_dict({}, rds, types)
    else:
        res = load(rds, types)
    return res

def save_rds(data, filename):
    import collections, re
    import pandas as pd
    import numpy as np
    import rpy2.robjects as RO
    import rpy2.rinterface as RI
    from rpy2.robjects import numpy2ri
    numpy2ri.activate()
    from rpy2.robjects import pandas2ri
    pandas2ri.activate()
    # Supported data types:
    # int, float, str, tuple, list, numpy array
    # numpy matrix and pandas dataframe
    def assign(name, value):
        name = re.sub(r'[^\w' + '_.' + ']', '_', name)
        if isinstance(value, (tuple, list)):
             if all(isinstance(item, int) for item in value):
                  value = np.asarray(value, dtype = int)
             elif all(isinstance(item, float) for item in value):
                  value = np.asarray(value, dtype = float)
             else:
                  value = np.asarray(value)
        if isinstance(value, np.matrix):
            value = np.asarray(value)
        if isinstance(value, (str, float, int, np.ndarray)):
            if isinstance(value, np.ndarray) and value.dtype.kind == "u":
                value = value.astype(int)
            RO.r.assign(name, value)
        elif isinstance(value, pd.DataFrame):
            # FIXME: does not always work well for pd.DataFrame
            RO.r.assign(name, value)
        elif value is None:
            RO.r.assign(name, RI.NULL)
        else:
            raise ValueError("Saving ``{}`` to RDS file is not supported!".format(str(type(value))))
    #
    def assign_dict(name, value):
        RO.r('%s <- list()' % name)
        for k, v in value.items():
            k = re.sub(r'[^\w' + '_.' + ']', '_', str(k))
            if k.isdigit():
                k = 'V' + k
            if isinstance(v, collections.Mapping):
                assign_dict('%s$%s' %(name, k), v)
            else:
                assign('item', v)
                RO.r('%s$%s <- item' % (name, k))
    #
    if isinstance(data, collections.Mapping):
        assign_dict('res', data)
    else:
        assign('res', data)
    RO.r("saveRDS(res, '%s')" % filename)

def load_dsc(infiles):
    import pickle, yaml
    if isinstance(infiles, str):
        infiles = [infiles]
    res = dict()
    for infile in infiles:
        if infile.endswith('.pkl'):
            data = pickle.load(open(infile, 'rb'))
        elif infile.endswith('.rds'):
            data = load_rds(infile)
        elif infile.endswith('.yml'):
            data = yaml.load(open(infile).read())
        else:
            raise ValueError(f'``{infile}`` is not supported DSC data format')
        try:
            res.update(data)
        except Exception:
            # loaded a non-recursive object
            return data
    return res

def convert_dsc(pkl_files, jobs = 2):
    import pickle
    from multiprocessing import Process
    from .utils import chunks
    def convert(d):
        for ff in d:
            if not ff.endswith('pkl'):
                raise ValueError(f'``{ff}`` is not supported DSC data format')
            save_rds(pickle.load(open(ff, 'rb')), ff[:-4] + '.rds')
    #
    if isinstance(pkl_files, str):
        convert([pkl_files])
        return 0
    #
    pkl_files = [x for x in chunks(pkl_files, int(len(pkl_files) / jobs) + 1)]
    job_pool = [Process(target = convert, args = (x,)) for x in pkl_files]
    for job in job_pool:
        job.start()
    for job in job_pool:
        job.join()
    return 0

def symlink_force(target, link_name):
    import os, errno
    try:
        os.symlink(target, link_name)
    except OSError as e:
        if e.errno == errno.EEXIST:
            os.remove(link_name)
            os.symlink(target, link_name)
        else:
            raise e

def main():
    import os, sys, pickle
    if len(sys.argv) < 3:
        sys.exit(0)
    from sos.targets import fileMD5
    # Input is pkl, output is rds
    infile = sys.argv[1]
    outfile = sys.argv[2]
    outfile_md5 = outfile + '-' + fileMD5(infile)
    if not os.path.isfile(outfile_md5):
        if infile.endswith('.pkl') and outfile.endswith('.rds'):
            save_rds(pickle.load(open(infile, 'rb')), outfile_md5)
        elif infile.endswith('.rds') and outfile.endswith('.pkl'):
            pickle.dump(load_rds(infile), open(outfile_md5, 'wb'))
        else:
            sys.exit(1)
    symlink_force(os.path.abspath(os.path.expanduser(outfile_md5)), outfile)

if __name__ == '__main__':
    import warnings
    from rpy2.rinterface import RRuntimeWarning
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category = RRuntimeWarning)
        main()
