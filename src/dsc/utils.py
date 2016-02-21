#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import sys, os, random, tempfile, logging, copy, re, itertools

class RuntimeEnvironments(object):
    # the following make RuntimeEnvironments a singleton class
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            # *args, **kwargs are not passed to avoid
            # DeprecationWarning: object.__new__() takes no parameters
            # cls._instance = super(Singleton, cls).__new__(cls, *args, **kwargs)
            cls._instance = super(RuntimeEnvironments, cls).__new__(cls) #, *args, **kwargs)
        return cls._instance

    def __init__(self):
        self._search_path = os.getcwd()
        self._logfile_verbosity = 0
        self._verbosity = 1
        # path to a temporary directory, will be allocated automatically.
        self._temp_dir = tempfile.mkdtemp()
        # a list of lock file that will be removed when the project is killed
        self._lock_files = []
        #
        self._term_width = None
        # logger
        self._logger = None

    def lock(self, filename, content=''):
        with open(filename, 'w') as lockfile:
            lockfile.write(content)
        self._lock_files.append(filename)

    def unlock(self, filename, content=''):
        if filename in self._lock_files:
            self._lock_files.remove(filename)
        if not os.path.isfile(filename):
            return
        with open(filename) as lockfile:
            if lockfile.read() != content:
                raise RuntimeError('Inconsistent lock file. The output file might have been changed by another process.')
        try:
            os.remove(filename)
        except Exception as e:
            self._logger.warning('Failed to remove lock file {}: {}'
                .format(filename, e))

    def unlock_all(self):
        for filename in self._lock_files:
            try:
                os.remove(filename)
            except Exception as e:
                self._logger.warning('Failed to remove lock file {}: {}'
                    .format(filename, e))
        self._lock_files = []
    #
    # attribute term_width
    #
    def _set_term_width(self, v):
        try:
            self._term_width = int(v)
        except:
            self._term_width = None
    #
    term_width = property(lambda self: self._term_width, _set_term_width)
    #
    # attribute logfile_verbosity
    #
    def _set_logfile_verbosity(self, v):
        if v in ['0', '1', '2']:
            self._logfile_verbosity = v
    #
    logfile_verbosity = property(lambda self: self._logfile_verbosity, _set_logfile_verbosity)
    #
    #
    # attribute verbosity
    #
    def _set_verbosity(self, v):
        if v in ['0', '1', '2', '3']:
            self._verbosity = v
    #
    verbosity = property(lambda self: self._verbosity, _set_verbosity)
    def _set_temp_dir(self, path=None):
        if path not in [None, 'None', '']:
            path = os.path.expanduser(path)
            if not os.path.isdir(path):
                raise ValueError('Temp directory {} does not exist'.format(path))
            if os.path.isdir(path) and (
                    (not os.access(path, os.R_OK)) or (not os.access(path, os.W_OK)) or
                    (os.stat(path).st_mode & stat.S_ISVTX == 512)):
                raise ValueError('Cannot set temporary directory to directory {} because '.format(path) + \
                    'it is not empty or is not writable or deletable. Please clear this directory or '
                    'set it to another path, or a random path (empty DIR).')
            self._temp_dir = path
            # create a random subdirectory in this directory
            while True:
                subdir = os.path.join(path, '_tmp_{}'.format(random.randint(1, 1000000)))
                if not os.path.isdir(subdir):
                    if self._proj_temp_dir is not None and os.path.isdir(self._proj_temp_dir):
                        try:
                            shutil.rmtree(env._proj_temp_dir)
                        except:
                            pass
                    self._proj_temp_dir = subdir
                    os.makedirs(subdir)
                    break
        else:
            # the usual case
            if self._temp_dir is None:
                self._proj_temp_dir = tempfile.mkdtemp()
            try:
                if not os.path.isdir(os.path.expanduser(self._proj_temp_dir)):
                    os.makedirs(os.path.expanduser(self._proj_temp_dir))
                while True:
                    subdir = os.path.join(self._proj_temp_dir, '_tmp_{}'.format(random.randint(1, 1000000)))
                    if not os.path.isdir(subdir):
                        if self._proj_temp_dir is not None and os.path.isdir(self._proj_temp_dir):
                            try:
                                shutil.rmtree(env._proj_temp_dir)
                            except:
                                pass
                        self._proj_temp_dir = subdir
                        os.makedirs(subdir)
                        break
            except:
                sys.stderr.write('Failed to create a temporary directory {}.\n'.format(self._proj_temp_dir))
                self._proj_temp_dir = tempfile.mkdtemp()
    #
    def _get_temp_dir(self):
        if self._proj_temp_dir is None:
            self._set_temp_dir()
        return os.path.expanduser(self._proj_temp_dir)
    #
    temp_dir = property(_get_temp_dir, _set_temp_dir)
        # attribute search_path
    def _set_search_path(self, val):
        if val not in ['None', None]:
            self._search_path = val
    #
    search_path = property(lambda self: self._search_path, _set_search_path)
    #
    # user stash
    def _set_user_stash(self, val):
        if val not in ['None', None]:
            self._user_stash = val
    #
    user_stash = property(lambda self: self._user_stash, _set_user_stash)
    #
    #
    # attribute logger
    class ColoredFormatter(logging.Formatter):
        # A variant of code found at http://stackoverflow.com/questions/384076/how-can-i-make-the-python-logging-output-to-be-colored
        def __init__(self, msg):
            logging.Formatter.__init__(self, msg)
            self.LEVEL_COLOR = {
                'TRACE': 'DARK_CYAN',
                'DEBUG': 'BLUE',
                'WARNING': 'PURPLE',
                'ERROR': 'RED',
                'CRITICAL': 'RED_BG',
                }
            self.COLOR_CODE={
                'ENDC':0,  # RESET COLOR
                'BOLD':1,
                'UNDERLINE':4,
                'BLINK':5,
                'INVERT':7,
                'CONCEALD':8,
                'STRIKE':9,
                'GREY30':90,
                'GREY40':2,
                'GREY65':37,
                'GREY70':97,
                'GREY20_BG':40,
                'GREY33_BG':100,
                'GREY80_BG':47,
                'GREY93_BG':107,
                'DARK_RED':31,
                'RED':91,
                'RED_BG':41,
                'LIGHT_RED_BG':101,
                'DARK_YELLOW':33,
                'YELLOW':93,
                'YELLOW_BG':43,
                'LIGHT_YELLOW_BG':103,
                'DARK_BLUE':34,
                'BLUE':94,
                'BLUE_BG':44,
                'LIGHT_BLUE_BG':104,
                'DARK_MAGENTA':35,
                'PURPLE':95,
                'MAGENTA_BG':45,
                'LIGHT_PURPLE_BG':105,
                'DARK_CYAN':36,
                'AUQA':96,
                'CYAN_BG':46,
                'LIGHT_AUQA_BG':106,
                'DARK_GREEN':32,
                'GREEN':92,
                'GREEN_BG':42,
                'LIGHT_GREEN_BG':102,
                'BLACK':30,
            }

        def colorstr(self, astr, color):
            return '\033[{}m{}\033[{}m'.format(self.COLOR_CODE[color], astr,
                self.COLOR_CODE['ENDC'])

        def emphasize(self, msg, in_color):
            # display text within `` and `` in green
            # This is done for levelname not in self.LEVEL_COLOR, e.g.
            # for info that uses native color. The text will not be
            # visible if someone is using a green background
            if in_color == 0:
                return re.sub(r'``([^`]*)``', '\033[32m\\1\033[0m', str(msg))
            else:
                return re.sub(r'``([^`]*)``', '\033[32m\\1\033[{}m'.format(self.COLOR_CODE[in_color]), str(msg))

        def format(self, record):
            record = copy.copy(record)
            levelname = record.levelname
            if levelname in self.LEVEL_COLOR:
                record.levelname = self.colorstr(levelname, self.LEVEL_COLOR[levelname])
                record.name = self.colorstr(record.name, 'BOLD')
                record.msg = self.colorstr(self.emphasize(record.msg,
                    self.LEVEL_COLOR[levelname]), self.LEVEL_COLOR[levelname])
            else:
                record.msg = self.emphasize(record.msg, 0)
            return logging.Formatter.format(self, record)

    def _set_logger(self, logfile=None):
        # create a logger, but shutdown the previous one
        if not hasattr(logging, 'TRACE'):
            logging.TRACE = 5
            logging.addLevelName(logging.TRACE, "TRACE")
        #
        if self._logger is not None:
            self._logger.handlers = []
        self._logger = logging.getLogger()
        self._logger.setLevel(logging.DEBUG)
        # output to standard output
        cout = logging.StreamHandler()
        levels = {
            0: logging.WARNING,
            1: logging.INFO,
            2: logging.DEBUG,
            3: logging.TRACE,
            None: logging.INFO
        }
        #
        cout.setLevel(levels[self._verbosity])
        cout.setFormatter(self.ColoredFormatter('%(levelname)s: %(message)s'))
        self._logger.addHandler(cout)
        self._logger.trace = lambda msg, *args: self._logger._log(logging.TRACE, msg, args)
        # output to a log file
        if logfile is not None:
            ch = logging.FileHandler(logfile.lstrip('>'), mode = ('a' if logfile.startswith('>>') else 'w'))
            # NOTE: debug informaiton is always written to the log file
            ch.setLevel(levels[self._logfile_verbosity])
            ch.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
            self._logger.addHandler(ch)
    #
    logger = property(lambda self: self._logger, _set_logger)

# the singleton object of RuntimeEnvironments
env = RuntimeEnvironments()
# create a default logger without logging to file
env.logger = None

def lower_keys(x, level_start = 0, level_end = 2):
    level_start += 1
    if level_start > level_end:
        return x
    if isinstance(x, list):
        return [lower_keys(v, level_start, level_end) for v in x]
    elif isinstance(x, dict):
        return dict((k.lower(), lower_keys(v, level_start, level_end)) for k, v in x.items())
    else:
        return x

class CheckRLibraries:
    def __init__(self, target):
        pass

def is_null(var):
    if var is None:
        return True
    if type(var) is str:
        if var.lower() in ['na','nan','null','none']:
            return True
    return False

def str2num(var):
    if type(var) is str:
        # try to warn about boolean
        if var in ['T', 'F'] or var.lower() in ['true', 'false']:
            bmap = {'t': 1, 'true': 1, 'f': 0, 'false': 0}
            msg = 'Possible Boolean variable detected: "{}". \n\
            This variable will be treated as string, not Boolean data. \n\
            It may cause problems to your jobs. \n\
            Please set this variable to {} if it is indeed Boolean data.'.format(var, bmap[var.lower()])
            env.logger.warning('\n\t'.join([x.strip() for x in msg.split('\n')]))
        try:
            return int(var)
        except ValueError:
            try:
                return float(var)
            except ValueError:
                return re.sub(r'''^"|^'|"$|'$''', "", var)
    else:
        return var

def cartesian_dict(value):
    return [dict(zip(value, x)) for x in itertools.product(*value.values())]

def cartesian_list(*args):
    return list(itertools.product(*args))

def pairwise_list(*args):
    return list(itertools.product(*args))

def flatten_list(lst):
    return sum( ([x] if not isinstance(x, list) else flatten_list(x) for x in lst), [] )

def get_slice(value):
    '''
    Input string is R index style: 1-based, end position inclusive slicing
    Output index will convert it to Python convention
    exe[1,2,4] ==> (exe, (0,1,3))
    exe[1] ==> (exe, (0))
    exe[1:4] ==> (exe, (0,1,2,3))
    exe[1:9:2] ==> (exe, (0,2,4,6,8))
    '''
    try:
        slicearg = re.search('\[(.*?)\]', value).group(1)
    except:
        raise ValueError('Cannot obtain slice from input string {}'.format(value))
    name = value.split('[')[0]
    if ',' in slicearg:
        return name, tuple(int(n.strip()) - 1 for n in slicearg.split(',') if n.strip())
    elif ':' in slicearg:
        slice_ints = [ int(n) for n in slicearg.split(':') ]
        if len(slice_ints) == 1:
            raise ValueError('Wrong syntax for slice {}.'.format(value))
        slice_ints[1] += 1
        slice_obj = slice(*tuple(slice_ints))
        return name, tuple(x - 1 for x in range(slice_obj.start or 0, slice_obj.stop or -1, slice_obj.step or 1))
    else:
        return name, int(slicearg.strip()) - 1
