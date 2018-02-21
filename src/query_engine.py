#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import os, re, pickle
import pandas as pd
from .utils import uniq_list, flatten_list, filter_sublist, FormatError, DBError, logger
from .yhat_sqldf import sqldf
from .line import parse_filter

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

def find_partial_index(xx, ordering):
    for ii, i in enumerate(ordering):
        if xx.startswith(i):
            return ii
    raise ValueError(f'{xx} not in list {ordering}')

class Query_Processor:
    def __init__(self, db, targets, condition = None, groups = None, add_path = False):
        self.db = db
        self.targets = targets
        self.raw_condition = condition
        self.data = pickle.load(open(os.path.expanduser(db), 'rb'))
        # table: msg map
        self.field_warnings = {}
        if '.groups' in self.data:
            self.groups = self.data['.groups']
        else:
            self.groups = dict()
        self.groups.update(self.get_grouped_tables(groups))
        self.target_tables = self.get_table_fields(targets)
        self.condition, self.condition_tables = parse_filter(condition, groups = self.groups)
        # 1. only keep tables that do exist in database
        self.target_tables = self.filter_tables(self.target_tables)
        self.condition_tables = self.filter_tables(self.condition_tables)
        # 2. identify all pipelines in database
        self.pipelines = self.get_pipelines()
        # 3. identify and extract which part of each pipeline are involved
        # based on tables in target / condition
        self.pipelines = self.filter_pipelines()
        # 4. make inner join, the FROM clause
        from_clauses = self.get_from_clause()
        # 5. make select / where clause
        select_clauses, where_clauses = self.get_select_where_clause()
        self.queries = [' '.join(x) for x in list(zip(*[select_clauses, from_clauses, where_clauses]))]
        # 6. run queries
        self.output_tables = self.run_queries(add_path)
        # 7. merge table
        self.output_table = self.merge_tables()
        # 8. show warnings
        self.warn()

    @staticmethod
    def legalize_name(name, kw = False):
        # FIXME: have to ensure keywords conflict is supported
        if name is None:
            return name
        output = ''
        for x in name:
            if re.match(r'^[a-zA-Z0-9_]+$', x):
                output += x
            else:
                output += '_'
        if re.match(r'^[0-9][a-zA-Z0-9_]+$', output) or (output.upper() in SQL_KEYWORDS and kw):
            output = '_' + output
        return output

    def check_table_field(self, value, check_field = 0):
        '''
        Input is (table, field)
        output is if they are valid
        check_field: zero for not check, 1 for check SELECT statement, 2 for check WHERE statement
        '''
        x, y = value
        if x != self.legalize_name(x):
            raise DBError(f"Invalid module specification ``{x}``")
        keys_lower = [k.lower() for k in self.data.keys()]
        if not x.lower() in keys_lower:
            raise DBError(f"Cannot find module ``{x}`` in DSC results ``{self.db}``.")
        k = list(self.data.keys())[keys_lower.index(x.lower())]
        y_low = y.lower()
        if y_low in [i.lower() for i in self.data[k]] and y_low in [i.lower() for i in self.data['.output'][k]] and check_field == 1:
            self.field_warnings[k] = f"Variable ``{y}`` is both parameter and output in module ``{k}``. Parameter variable ``{y}`` is extracted. To obtain output variable ``{y}`` please use ``{k}.output.{y}`` to specify the query target."
        if not y_low in [i.lower() for i in self.data[k]] and check_field == 2:
            raise DBError(f"Cannot find column ``{y}`` in table ``{k}``")
        if y_low.startswith('output.'):
            y_low = y_low[7:]
        if not y_low in [i.lower() for i in self.data[k]] and not y_low in [i.lower() for i in self.data['.output'][k]] and check_field == 1:
            raise DBError(f"Cannot find variable ``{y}`` in module ``{k}``")
        return

    @staticmethod
    def get_grouped_tables(groups):
        '''
        input is g: m1, m2
        output is {g: [m1, m2]}
        '''
        if groups is None:
            return []
        res = dict()
        for g in groups:
            if len(g.split(':')) != 2:
                raise FormatError(f"Illegal module group option ``{g}``. Please use format ``group: module1, module2``")
            g = tuple(x.strip() for x in g.split(':'))
            v = uniq_list([x.strip() for x in g[1].split(',')])
            if g[0] in v:
                raise FormatError(f"Invalid group option: module group name ``{g[0]}``conflicts with module name ``{g[0]}``.")
            res[g[0]] = v
        return res

    def get_table_fields(self, values):
        '''
        input is lists of strings
        output should be lists of tuples
        [(table, field), (table, field) ...]
        '''
        res = []
        for item in ' '.join(values).split():
            if re.search('^\w+\.\w+$', item) or  re.search('^\w+\.output.\w+$', item):
                item, y = item.split('.', 1)
                if not y:
                    raise FormatError(f"Field for module ``{item}`` is empty.")
            else:
                y = '__output__'
            if item in self.groups:
                item = self.groups[item]
            else:
                item = [item]
            for x in item:
                self.check_table_field((x, y), 1)
                res.append((x, y))
        return res

    def filter_tables(self, tables):
        return uniq_list([x for x in tables if x[0].lower() in
                          [y.lower() for y in self.data.keys()
                           if not y.startswith('pipeline_') and not y.startswith('.')]])

    def get_pipelines(self):
        '''
        example output:
        [('rnorm', 'mean', 'MSE'), ('rnorm', 'median', 'MSE'), ... ('rt', 'winsor', 'MSE')]
        '''
        res = [[tuple(kk.split('+')) for kk in self.data[k].keys()]
               for k in self.data.keys() if k.startswith("pipeline_")]
        return sum(res, [])

    def filter_pipelines(self):
        '''
        for each pipeline extract the sub pipeline that the query involves
        '''
        res = []
        tables = uniq_list([x[0].lower() for x in self.target_tables] + [x[0].lower() for x in self.condition_tables])
        for pipeline in self.pipelines:
            pidx = [l[0] for l in enumerate(pipeline) if l[1] in tables]
            if len(pidx) and not pipeline[pidx[0]:pidx[-1]+1] in res:
                res.append(pipeline[pidx[0]:pidx[-1]+1])
        return filter_sublist(res)

    def get_from_clause(self):
        res = []
        for pipeline in self.pipelines:
            pipeline = list(reversed(pipeline))
            res.append('FROM {0} '.format(pipeline[0]) + ' '.join(["INNER JOIN {1} ON {0}.__parent__ = {1}.__id__".format(pipeline[i], pipeline[i+1]) for i in range(len(pipeline) - 1)]))
        return res

    def get_select_where_clause(self):
        select = []
        where = []
        select_fields = []
        for pipeline in self.pipelines:
            tmp1 = []
            tmp2 = []
            for item in self.target_tables:
                if len([x for x in pipeline if x.lower() == item[0].lower()]) == 0:
                    continue
                tmp2.append(item)
                if item[1] is None:
                    tmp1.append("'{0}' AS {0}".format(item[0]))
                    continue
                key = [x for x in self.data.keys() if x.lower() == item[0].lower()][0]
                if item[1].lower() not in [x.lower() for x in self.data[key].keys()]:
                    tmp1.append("{0}.__output__ AS {0}___output___{1}".format(item[0], item[1] if not item[1].startswith('output.') else item[1][7:]))
                else:
                    tmp1.append("{0}.{1} AS {0}_{1}".format(item[0], item[1]))
            select.append("SELECT " + ', '.join(tmp1))
            where.append(self.get_one_where_clause([x[0].lower() for x in tmp2]))
            select_fields.append(['.'.join(x) for x in tmp2])
        # not all pipelines will be used
        # because of `-t` option logic, if a new
        output_fields = filter_sublist(select_fields, ordered = False)
        select = [x for i, x in enumerate(select) if select_fields[i] in output_fields]
        where = [x for i, x in enumerate(where) if select_fields[i] in output_fields]
        return select, where

    def get_one_where_clause(self, tables):
        '''
        After expanding, condition is a list of list
        the outer lists are connected by OR
        the inner lists are connected by AND
        '''
        # to decide which part of the conditions is relevant to which pipeline we have to
        # dissect it to reveal table/field names
        condition = []
        for each_and in self.condition:
            tmp = []
            for value in each_and:
                if isinstance(value, tuple):
                    self.check_table_field(value[1], 2)
                    value = [value]
                else:
                    for vv in value:
                        self.check_table_field(vv[1], 2)
                valid_idx = [idx for idx, vv in enumerate(value) if vv[1][0].lower() in tables]
                if len(valid_idx) >= 1:
                    value = ' OR '.join([f"{value[i][0]} ({'.'.join(value[i][1])} {value[i][2]} {value[i][3]})" if len(value[i][0]) else f"{'.'.join(value[i][1])} {value[i][2]} {value[i][3]}" for i in valid_idx])
                    if len(valid_idx) > 1:
                        tmp.append(f"({value})")
                    else:
                        tmp.append(value)
                else:
                    pass
            condition.append(tmp)
        if len(condition) > 0:
            return "WHERE " + ' OR '.join(['(' + ' AND '.join([f"({y})" for y in x]) + ')' for x in condition])
        else:
            return ''

    def adjust_table(self, table, ordering = None, add_path = False):
        if len(table) == 0:
            return None
        table = pd.DataFrame(table)
        rename = {x: x.replace('___output___', '.').replace('___output__', '') + '.output' for x in table if "___output__" in x}
        if ordering is None:
            table = table[sorted([x for x in table if not "___output__" in x]) + \
                          sorted([x for x in table if "___output__" in x])].rename(columns = rename)
        else:
            table = table[sorted(table.columns, key = lambda x: find_partial_index(x, ordering))].rename(columns = rename)
        if add_path:
            for x in table:
                if x.endswith(".output"):
                    table[x] = table[x].apply(lambda i: os.path.join(os.path.dirname(self.db), i))
        return table

    def merge_tables(self):
        common_keys = [t.columns for t in self.output_tables.values()]
        common_keys = list(set(common_keys[0]).intersection(*common_keys))
        table = pd.concat(self.output_tables.values(), join = 'outer', ignore_index = True)
        to_drop = []
        for g in self.groups:
            # For each group, find common fields to merge
            to_merge = dict()
            gvals = sorted(self.groups[g], key = len, reverse = True)
            for col in table.columns:
                for k in gvals:
                    if not (col.startswith(k + '_') or col.startswith(k + '.')):
                        continue
                    k = col[len(k):]
                    if not k in to_merge:
                        to_merge[k] = []
                    to_merge[k].append(col)
                    break
            for k in to_merge:
                if len(to_merge[k]) > 1:
                    table[f'{g}{k}'] = table.loc[:, to_merge[k]].apply(lambda x: x.dropna().tolist(), 1)
                    if not all(table[f'{g}{k}'].apply(len) == 1):
                        raise DBError(f'Modules ``to_merge[k]`` cannot be grouped into ``{g}{k}`` due to collating entries.')
                    table[f'{g}{k}'] = table[f'{g}{k}'].apply(lambda x: x[0])
                    if not g in table:
                        table[g] = None
                        for col in to_merge[k]:
                            table[g] = table.apply(lambda row: [kk for kk in gvals if col.startswith(kk + '_') or col.startswith(kk + '.')][0]
                                                   if not row[col] == row[col] else row[g], axis = 1)
                else:
                    # simply rename it
                    table[f'{g}{k}'] = table[to_merge[k][0]]
                    table[g] = [kk for kk in gvals if to_merge[k][0].startswith(kk + '_') or to_merge[k][0].startswith(kk + '.')][0]
            to_drop.extend(to_merge.values())
        #
        table.drop(set(sum(to_drop, [])), axis=1, inplace=True)
        # Adjust column name / ordering
        targets = uniq_list([x.split('.', 1)[0] for x in self.targets])
        targets = flatten_list([[x] + self.groups[x] if x in self.groups else x for x in targets])
        table = table.rename(columns = {g: f'{g}.id' for g in self.groups})
        table = table[sorted(table.columns, key = lambda x: (find_partial_index(x, targets), not x.endswith('.id')))]
        table = table.rename(columns = {f'{g}.id': g for g in self.groups})
        return table

    def get_queries(self):
        return self.queries

    def get_data(self):
        return self.data

    def run_queries(self, add_path = False):
        res = [('+'.join(pipeline), self.adjust_table(sqldf(query, self.data), pipeline, add_path)) \
                     for pipeline, query in zip(self.pipelines, self.queries)]
        res = [x for x in res if x[1] is not None]
        if len(res) == 0:
            raise DBError("No results found for targets ``{}``{}".\
                          format(', '.join(self.targets),
                                 f' under condition ``{" AND ".join(["(%s)" % x for x in self.raw_condition])}``' if self.raw_condition is not None else ''))
        return dict(res)

    def warn(self):
        for k in self.field_warnings:
            logger.warning(self.field_warnings[k])

if __name__ == '__main__':
    import sys
    q = Query_Processor(sys.argv[1], [sys.argv[2]], [sys.argv[3]])
    print(q.queries)
