#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import sys, os, random, copy, re, itertools,\
  yaml, collections, hashlib, time, sqlite3,\
  csv
from io import StringIO
import rpy2.robjects as RO
from pysos.utils import env

SQL_KEYWORDS = set([
    'ADD', 'ALL', 'ALTER', 'ANALYZE', 'AND', 'AS', 'ASC', 'ASENSITIVE', 'BEFORE',
    'BETWEEN', 'BIGINT', 'BINARY', 'BLOB', 'BOTH', 'BY', 'CALL', 'CASCADE', 'CASE',
    'CHANGE', 'CHAR', 'CHARACTER', 'CHECK', 'COLLATE', 'COLUMN', 'CONDITION',
    'CONSTRAINT', 'CONTINUE', 'CONVERT', 'CREATE', 'CROSS', 'CURRENT_DATE',
    'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER', 'CURSOR', 'DATABASE',
    'DATABASES', 'DAY_HOUR', 'DAY_MICROSECOND', 'DAY_MINUTE', 'DAY_SECOND', 'DEC',
    'DECIMAL', 'DECLARE', 'DEFAULT', 'DELAYED', 'DELETE', 'DESC',
    'DESCRIBE', 'DETERMINISTIC', 'DISTINCT', 'DISTINCTROW', 'DIV', 'DOUBLE',
    'DROP', 'DUAL', 'EACH', 'ELSE', 'ELSEIF', 'ENCLOSED', 'ESCAPED', 'EXISTS',
    'EXIT', 'EXPLAIN', 'FALSE', 'FETCH', 'FLOAT', 'FLOAT4', 'FLOAT8', 'FOR',
    'FORCE', 'FOREIGN', 'FROM', 'FULLTEXT', 'GRANT', 'GROUP', 'HAVING', 'HIGH_PRIORITY',
    'HOUR_MICROSECOND', 'HOUR_MINUTE', 'HOUR_SECOND', 'IF', 'IGNORE', 'IN',
    'INDEX', 'INFILE', 'INNER', 'INOUT', 'INSENSITIVE', 'INSERT',
    'INT', 'INT1', 'INT2', 'INT3', 'INT4', 'INT8', 'INTEGER', 'INTERVAL', 'INTO',
    'IS', 'ITERATE', 'JOIN', 'KEY', 'KEYS', 'KILL', 'LEADING', 'LEAVE', 'LEFT',
    'LIKE', 'LIMIT', 'LINES', 'LOAD', 'LOCALTIME', 'LOCALTIMESTAMP',
    'LOCK', 'LONG', 'LONGBLOB', 'LONGTEXT', 'LOOP', 'LOW_PRIORITY', 'MATCH',
    'MEDIUMBLOB', 'MEDIUMINT', 'MEDIUMTEXT', 'MIDDLEINT', 'MINUTE_MICROSECOND',
    'MINUTE_SECOND', 'MOD', 'MODIFIES', 'NATURAL', 'NOT', 'NO_WRITE_TO_BINLOG',
    'NULL', 'NUMERIC', 'ON', 'OPTIMIZE', 'OPTION', 'OPTIONALLY', 'OR',
    'ORDER', 'OUT', 'OUTER', 'OUTFILE', 'PRECISION', 'PRIMARY', 'PROCEDURE',
    'PURGE', 'READ', 'READS', 'REAL', 'REFERENCES', 'REGEXP', 'RELEASE',
    'RENAME', 'REPEAT', 'REPLACE', 'REQUIRE', 'RESTRICT', 'RETURN',
    'REVOKE', 'RIGHT', 'RLIKE', 'SCHEMA', 'SCHEMAS', 'SECOND_MICROSECOND',
    'SELECT', 'SENSITIVE', 'SEPARATOR', 'SET', 'SHOW', 'SMALLINT',
    'SONAME', 'SPATIAL', 'SPECIFIC', 'SQL', 'SQLEXCEPTION', 'SQLSTATE',
    'SQLWARNING', 'SQL_BIG_RESULT', 'SQL_CALC_FOUND_ROWS', 'SQL_SMALL_RESULT',
    'SSL', 'STARTING', 'STRAIGHT_JOIN', 'TABLE', 'TERMINATED',
    'THEN', 'TINYBLOB', 'TINYINT', 'TINYTEXT', 'TO', 'TRAILING',
    'TRIGGER', 'TRUE', 'UNDO', 'UNION', 'UNIQUE', 'UNLOCK', 'UNSIGNED',
    'UPDATE', 'USAGE', 'USE', 'USING', 'UTC_DATE', 'UTC_TIME', 'UTC_TIMESTAMP', 'VALUES',
    'VARBINARY', 'VARCHAR', 'VARCHARACTER', 'VARYING', 'WHEN', 'WHERE', 'WHILE',
    'WITH', 'WRITE', 'XOR', 'YEAR_MONTH', 'ZEROFILL', 'ASENSITIVE', 'CALL', 'CONDITION',
    'CONNECTION', 'CONTINUE', 'CURSOR', 'DECLARE', 'DETERMINISTIC', 'EACH',
    'ELSEIF', 'EXIT', 'FETCH', 'GOTO', 'INOUT', 'INSENSITIVE', 'ITERATE', 'LABEL', 'LEAVE',
    'LOOP', 'MODIFIES', 'OUT', 'READS', 'RELEASE', 'REPEAT', 'RETURN', 'SCHEMA', 'SCHEMAS',
    'SENSITIVE', 'SPECIFIC', 'SQL', 'SQLEXCEPTION', 'SQLSTATE', 'SQLWARNING', 'TRIGGER',
    'UNDO', 'UPGRADE', 'WHILE', 'ABS', 'ACOS', 'ADDDATE', 'ADDTIME', 'ASCII', 'ASIN',
    'ATAN', 'AVG', 'BETWEEN', 'AND', 'BINARY', 'BIN', 'BIT_AND',
    'BIT_OR', 'CASE', 'CAST', 'CEIL', 'CHAR', 'CHARSET', 'CONCAT', 'CONV', 'COS', 'COT',
    'COUNT', 'DATE', 'DAY', 'DIV', 'EXP', 'IS', 'LIKE', 'MAX', 'MIN', 'MOD', 'MONTH',
    'LOG', 'POW', 'SIN', 'SLEEP', 'SORT', 'STD', 'VALUES', 'SUM'
])

class SQLiteMan:
    def __init__(self, dbpath):
        self.conn = sqlite3.connect(dbpath)
        try:
            # gcc -fPIC -lm -shared extension-functions.c -o libsqlitefunctions.so
            self.conn.enable_load_extension(True)
        except:
            pass
        self.c = self.conn.cursor()
        self.c.execute('pragma synchronous=off')
        self.c.execute('pragma count_changes=off')
        self.c.execute('pragma journal_mode=memory')
        self.c.execute('pragma temp_store=memory')

    def convert(self, fo, table = 'data', delim = None, header_option = None, force = False):
        # @author: Rufus Pollock
        # Placed in the Public Domain
        table = self._legalize_name(table)
        if table in self.getTables():
            if force:
                self.c.execute('DROP TABLE IF EXISTS {}'.format(table))
            else:
                sys.exit("Table '{}' already exists!".format(table))
        # guess delimiter
        if delim is None:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(fo.readline())
            delim = dialect.delimiter
            fo.seek(0)
        #
        if delim == '\\t': delim = '\t'
        reader = csv.reader(fo, delimiter = delim)

        if header_option is None:
            # first line is header
            headers = [self._legalize_name(x) for x in next(reader)]
            fo.seek(0)
            start = len(fo.readline())
        elif header_option is False:
            # no header
            headers = ["V{}".format(i+1) for i in range(len(next(reader)))]
            start = 0
        else:
            # use this input as header
            if header_option == ['-']:
                headers = sys.stdin.readlines()[0].strip().split(delim if delim != '\t' else '\\t')
            else:
                headers = header_option
            start = 0
        #
        for idx, header in enumerate(headers):
            if header.upper() in SQL_KEYWORDS:
                headers[idx] = "_" + header
        #
        types = self._guess_types(reader, headers)
        fo.seek(start)

        _columns = ','.join(
            ['"%s" %s' % (header, _type) for (header,_type) in zip(headers, types)]
            )

        self.c.execute('CREATE table %s (%s)' % (table, _columns))

        _insert_tmpl = 'insert into %s values (%s)' % (table,
            ','.join(['?']*len(headers)))
        for row in reader:
            # we need to take out commas from int and floats for sqlite to
            # recognize them properly ...
            row = [ x.replace(',', '') if y in ['real', 'integer'] else x
                    for (x,y) in zip(row, types) ]
            self.c.execute(_insert_tmpl, row)

        fo.close()
        # Set all empty strings to null
        for field in self.getFields(table):
            self.c.execute('update {0} set {1} = NULL where {1} = ""'.format(table, field[0]))
        self.conn.commit()
        self.c.close()

    def _guess_types(self, reader, headers, max_sample_size=100):
        '''Guess column types (as for SQLite) of CSV.
        '''
        # @author: Rufus Pollock
        # Placed in the Public Domain
        # we default to text for each field
        types = ['text'] * len(headers)
        # order matters
        # (order in form of type you want used in case of tie to be last)
        options = [
            ('text', str),
            ('real', float),
            ('integer', int)
            # 'date',
            ]
        # for each column a set of bins for each type counting successful casts
        perresult = {
            'integer': 0,
            'real': 0,
            'text': 0
            }
        results = [ dict(perresult) for x in range(len(headers)) ]
        for count,row in enumerate(reader):
            for idx,cell in enumerate(row):
                cell = cell.strip()
                # replace ',' with '' to improve cast accuracy for ints and floats
                cell = cell.replace(',', '')
                for key,cast in options:
                    try:
                        # for null cells we can assume success
                        if cell:
                            cast(cell)
                        results[idx][key] = (results[idx][key]*count + 1) / float(count+1)
                    except ValueError as inst:
                        pass
            if count >= max_sample_size:
                break
        for idx,colresult in enumerate(results):
            for _type, dontcare in options:
                if colresult[_type] == 1.0:
                    types[idx] = _type
        return types

    def _legalize_name(self, name):
        output = ''
        for x in name:
            if re.match(r'^[a-zA-Z0-9_]+$', x):
                output += x
            else:
                output += '_'
        if re.match(r'^[0-9][a-zA-Z0-9_]+$', output) or output.upper() in SQL_KEYWORDS:
            output = '_' + output
        return output

    def getTables(self):
        tables = []
        for item in self.c.execute("SELECT tbl_name FROM sqlite_master"):
            tables.extend(item)
        return sorted(tables)

    def getFields(self, table):
        fields = []
        for item in self.c.execute("PRAGMA table_info('{0}')".format(table)):
            fields.append((item[1].lower(), item[2]))
        return sorted(fields)

    def execute(self, query, display = True, delimiter = ','):
        text = ''
        for item in self.c.execute(query).fetchall():
            if len([x for x in item if x]) > 0:
                text += delimiter.join(map(str, item)) + '\n'
        self.conn.commit()
        self.c.close()
        text = text.strip()
        if display:
            print(text)
        return text

    def load_extension(self, ext):
        try:
            self.conn.load_extension(ext)
        except Exception as e:
            env.logger.error('Cannot load extension ``{}`` ({})! Perhaps python-sqlite3 version too old?\n'.\
                             format(ext,e))

class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs
        if self.verbose:
            env.logger.info('Elapsed time ``%.03f`` seconds.' % self.secs)

def lower_keys(x, level_start = 0, level_end = 2, mapping = dict):
    level_start += 1
    if level_start > level_end:
        return x
    if isinstance(x, list):
        return [lower_keys(v, level_start, level_end) for v in x]
    elif isinstance(x, colletions.Mapping):
        return mapping((k.lower(), lower_keys(v, level_start, level_end)) for k, v in x.items())
    else:
        return x

def is_null(var):
    if var is None:
        return True
    if type(var) is str:
        if var.lower() in ['na','nan','null','none','']:
            return True
    if isinstance(var, (list, tuple)):
        return True if len(var) == 0 else False
    return False

def str2num(var):
    if type(var) is str:
        # try to warn about boolean
        if var in ['T', 'F'] or var.lower() in ['true', 'false']:
            bmap = {'t': 1, 'true': 1, 'f': 0, 'false': 0}
            msg = 'Possible Boolean variable detected: ``{}``. \n\
            This variable will be treated as string, not Boolean data. \n\
            It may cause problems to your jobs. \n\
            Please set this variable to ``{}`` if it is indeed Boolean data.'.format(var, bmap[var.lower()])
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

def cartesian_dict(value, mapping = dict):
    return [mapping(zip(value, x)) for x in itertools.product(*value.values())]

def cartesian_list(*args):
    return list(itertools.product(*args))

def pairwise_list(*args):
    if len(set(len(x) for x in args)) > 1:
        raise ValueError("Cannot perform pairwise operation because input vectors are not of equal lengths.")
    return list(map(tuple, zip(*args)))

def flatten_list(lst):
    return sum( ([x] if not isinstance(x, list) else flatten_list(x) for x in lst), [] )

def flatten_dict(d, mapping = dict):
    if not isinstance(d, collections.Mapping):
        return d
    items = []
    for k, v in d.items():
        if isinstance(v, collections.Mapping):
            items.extend(flatten_dict(v).items())
        else:
            items.append((k, v))
    return mapping(items)

def uniq_list(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

def get_slice(value, all_tuple = True, mismatch_quit = True):
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
        if mismatch_quit:
            raise AttributeError('Cannot obtain slice from input string {}'.format(value))
        else:
            return value, None
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
        if all_tuple:
            return name, tuple([int(slicearg.strip()) - 1])
        else:
            return name, int(slicearg.strip()) - 1

def try_get_value(value, keys):
    '''
    Input: dict_data, (key1, key2, key3 ...)
    Output: dict_data[key1][key2][key3][...] or None
    '''
    if not isinstance(keys, (list, tuple)):
        keys = [keys]
    try:
        if len(keys) == 0:
            return value
        else:
            return try_get_value(value[keys[0]], keys[1:])
    except KeyError:
        return None

def set_nested_value(d, keys, value, default_factory = dict):
    """
    Equivalent to `reduce(dict.get, keys, d)[newkey] = newvalue`
    if all `keys` exists and corresponding values are of correct type
    """
    for key in keys[:-1]:
        try:
            val = d[key]
        except KeyError:
            val = d[key] = default_factory()
        else:
            if not isinstance(val, collections.MutableMapping):
                val = d[key] = default_factory()
        d = val
    d[keys[-1]] = value

def dict2str(value, replace = []):
    out = StringIO()
    yaml.dump(value, out, default_flow_style=False)
    res = out.getvalue()
    out.close()
    for item in replace:
        res = res.replace(item[0], item[1])
    return res

def update_nested_dict(d, u, mapping = dict):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = update_nested_dict(d.get(k, mapping()), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d

def strip_dict(data, mapping = dict):
    mapping_null = mapping()
    new_data = mapping()
    for k, v in data.items():
        if isinstance(v, collections.Mapping):
            v = strip_dict(v)
        if not v in ('', None, {}, [], mapping_null):
            new_data[k] = v
    return new_data

class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __setattr__ = dict.__setitem__
    __getattr__ = dict.__getitem__
    __delattr__ = dict.__delitem__

    def __deepcopy__(self, memo):
        return dotdict(copy.deepcopy(dict(self)))

class REncoder:
    """Encoding Pyton data structures into R."""
    @classmethod
    def encode_value(cls, value):
        if isinstance(value, list):
            return cls.encode_list(value)
        elif isinstance(value, dict):
            return cls.encode_dict(value)
        elif isinstance(value, str):
            return repr(value)
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, int) or isinstance(value, float):
            return str(value)
        else:
            raise ValueError(
                "Unsupported value for conversion into R: {}".format(value))

    @classmethod
    def encode_list(cls, l):
        return "c({})".format(", ".join(map(cls.encode_value, l)))

    @classmethod
    def encode_items(cls, items):
        def encode_item(item):
            name, value = item
            return '"{}" = {}'.format(name, cls.encode_value(value))

        return ", ".join(map(encode_item, items))

    @classmethod
    def encode_dict(cls, d):
        d = "list({})".format(cls.encode_items(d.items()))
        return d

    @classmethod
    def encode_namedlist(cls, namedlist):
        positional = cls.encode_list(namedlist)
        named = cls.encode_items(namedlist.items())
        source = "list("
        if positional != "c()":
            source += positional
        if named:
            source += ", " + named
        source += ")"
        return source

def registered_output(values, db_name):
    def register(base, md5):
        if ':%%:' in base:
            params, depends = base.split(':%%:')
        else:
            params = base
            depends = None
        md5 = md5.rsplit('.', 1)[0]
        text = '{}:\n'.format(md5 if depends is None else '{}_{}'.format(md5, depends))
        for item in params.split(':%:'):
            i, j = item.split('=')
            text += '    {}: {}\n'.format(i, j)
        return text
    #
    if isinstance(values, str):
        values = [values]
    res = []
    registry = ''
    for value in values:
        base, ext = value.rsplit('.', 1)
        md5 = '{}.{}'.format(hashlib.md5(base.encode('utf-8')).hexdigest() if sys.version_info[0] == 3 else hashlib.md5(base).hexdigest(), ext)
        res.append('{}/{}'.format(db_name, md5))
        registry += register(base, md5)
    try:
        with open('.sos/.dsc/.{}.tmp'.format(db_name), 'a') as f:
            f.write(registry)
    except IOError:
        pass
    return res

def sos_paired_input(values):
    '''Input must be a list of two lists,
    the lists are ordered such that the length of the
    2nd list is always multiples of the previous list.
    The way lists are supposed to be combined is:
    ABCD              AABBCCDD
    ABCDEFGH -------> ABCDEFGH ------> AABBCCDDABCDEFGH
    '''
    if len(values) != 2:
        raise ValueError("Input must be a pair of vectors!")
    multiplier = len(values[1]) / len(values[0])
    if multiplier > int(multiplier):
        # is not integer
        raise ValueError('Length of the 2nd list must be multiple of the 1st.')
    else:
        multiplier = int(multiplier)
    values[0] = flatten_list([[x for y in range(multiplier)] for x in values[0]])
    return flatten_list(values)

readRDS = RO.r['readRDS']

def load_rds(filename):
    rds = readRDS(filename)
    return dict(zip(rds.names, map(list, list(rds))))

def round_print(text, sep, pc = None):
    if pc is None:
        print(text)
        return
    for line in text.split('\n'):
        line = line.rstrip().split(sep)
        for i in range(len(line)):
            try:
                line[i] = int(line[i])
            except:
                try:
                    line[i] = float(line[i])
                except:
                    pass
        print (sep.join([('{0:.'+ str(pc) + 'E}').format(x) if isinstance(x, float) else str(x) for x in line]).strip())
