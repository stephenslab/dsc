#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import sys, os, msgpack, yaml, re, glob, pickle
from collections import OrderedDict
import pandas as pd
from sos.utils import logger
from .dsc_parser import DSC_Script
from .dsc_database import ResultDBError
from .utils import load_rds, save_rds, \
     flatten_list, uniq_list, no_duplicates_constructor, \
     cartesian_list, extend_dict, strip_dict, \
     try_get_value
from .yhat_sqldf import sqldf

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

class Query_Processor:
    def __init__(self, db, target, condition):
        self.target = target
        self.condition = condition
        self.target_tables = self.parse_tables(target, allow_null = True)
        self.condition_tables = self.parse_tables(condition, allow_null = False)
        self.data = pickle.load(open(os.path.expanduser(db), 'rb'))

    @staticmethod
    def parse_tables(values, allow_null):
        '''
        input is lists of strings
        output should be lists of tuples
        [(table, field), (table, field) ...]
        '''
        res = []
        for item in ' '.join(values).split():
            if re.search('^\w+\.\w+$', item):
                x, y = item.split('.')
                if x == self.legalize_name(x) and y == self.legalize_name(y):
                    res.append((x, y))
            else:
                if allow_null and item == self.legalize_name(item, kw = True):
                    res.append((item, None))
        return res

    @staticmethod
    def legalize_name(self, name, kw = False):
        output = ''
        for x in name:
            if re.match(r'^[a-zA-Z0-9_]+$', x):
                output += x
            else:
                output += '_'
        if re.match(r'^[0-9][a-zA-Z0-9_]+$', output) or (output.upper() in SQL_KEYWORDS and kw):
            output = '_' + output
        return output

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
