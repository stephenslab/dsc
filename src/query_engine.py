#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import os, re, pickle
import pandas as pd, numpy as np
from .utils import uniq_list, case_insensitive_uniq_list, flatten_list, filter_sublist, FormatError, DBError, logger
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
    if xx.split('.')[1] == 'DSC_REPLICATE':
        return -1
    raise ValueError(f'{xx} not in list {ordering}')

class Query_Processor:
    def __init__(self, db, targets, condition = None, groups = None):
        self.db = db
        self.targets = uniq_list(' '.join(targets).split())
        self.raw_condition = condition
        self.data = pickle.load(open(os.path.expanduser(db), 'rb'))
        # table: msg map
        self.field_warnings = {}
        if '.groups' in self.data:
            self.groups = self.data['.groups']
        else:
            self.groups = dict()
        self.groups.update(self.get_grouped_tables(groups))
        self.check_overlapping_groups()
        # 0. Fix the case when some module in the group has some parameter but others do not
        # changes will be applied to self.data
        self.add_na_group_parameters()
        self.target_tables = self.get_table_fields(self.targets)
        self.condition, self.condition_tables = parse_filter(condition, groups = self.groups)
        # 1. only keep tables that do exist in database
        self.target_tables = self.filter_tables(self.target_tables)
        self.condition_tables = self.filter_tables(self.condition_tables)
        # 2. identify all pipelines in database
        self.pipelines = self.get_pipelines()
        # 3. identify and extract which part of each pipeline are involved
        # based on tables in target / condition
        self.pipelines, self.first_modules = self.filter_pipelines()
        # 4. make select / from / where clause
        select_clauses, select_fields = self.get_select_clause()
        from_clauses = self.get_from_clause()
        where_clauses = self.get_where_clause(select_fields)
        self.queries = [' '.join(x) for x in list(zip(*[select_clauses, from_clauses, where_clauses]))]
        # 5. run queries
        self.output_tables = self.run_queries()
        # 6. merge table
        self.output_table = self.merge_tables()
        # 7. fillna
        self.fillna()
        # finally show warnings
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
        if y_low == 'dsc_replicate':
            raise DBError(f'Cannot query on ``DSC_REPLICATE`` in module ``{k}``')
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
            v = uniq_list([x.strip() for x in g[1].split(',') if x.strip()])
            if g[0] in v:
                raise FormatError(f"Invalid group option: module group name ``{g[0]}``conflicts with module name ``{g[0]}``.")
            res[g[0]] = v
        return res

    def check_overlapping_groups(self):
        for k in list(self.groups.keys()):
            if len(self.groups[k]) == 0:
                del self.groups[k]
        for i, k1 in enumerate(self.groups.keys()):
            for j, k2 in enumerate(self.groups.keys()):
                if i > j:
                    overlap = set(self.groups[k1]).intersection(set(self.groups[k2]))
                    if len(overlap):
                        raise DBError(f"Overlapping groups ``{k1} = {self.groups[k1]}`` and ``{k2} = {self.groups[k2]}`` not allowed! You should drop the one that causes the conflict, or use, eg, -g \"{k1}:\" to erase the other one if it is build-in.")

    def add_na_group_parameters(self):
        if len(self.groups) == 0:
            return
        for group in self.groups:
            params = uniq_list(flatten_list([self.data[item].columns.tolist() for item in self.groups[group]]))
            params = [x for x in params if not x in ['__id__', '__parent__', '__output__', 'DSC_REPLICATE']]
            for param in params:
                for module in self.groups[group]:
                    if param not in self.data[module].columns:
                        self.data[module][param] = np.nan

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
        heads = []
        tables = case_insensitive_uniq_list([x[0] for x in self.target_tables] + [x[0] for x in self.condition_tables])
        for pipeline in self.pipelines:
            pidx = [l[0] for l in enumerate(pipeline) if l[1] in tables]
            if len(pidx) == 0:
                continue
            # The first module contains replicate info and have to show up
            if pidx[0] != 0:
                pidx = [0] + pidx
            if not pipeline[pidx[0]:pidx[-1]+1] in res:
                res.append(pipeline[pidx[0]:pidx[-1]+1])
                heads.append(pipeline[0])
        res_filtered = filter_sublist(res)
        heads = [heads[i] for i in range(len(heads)) if res[i] in res_filtered]
        return res_filtered, heads

    def get_from_clause(self):
        res = []
        for pipeline in self.pipelines:
            pipeline = list(reversed(pipeline))
            res.append(('FROM {0} '.format(pipeline[0]) + ' '.join(["INNER JOIN {1} ON {0}.__parent__ = {1}.__id__".format(pipeline[i], pipeline[i+1]) for i in range(len(pipeline) - 1)])).strip())
        return res

    def get_one_select_clause(self, pipeline, first_module):
        clause = []
        fields = []
        # remove table in targets not exist in this pipeline
        tables = [item for item in self.target_tables if item[0] in pipeline]
        if len(tables):
            tables = [(first_module, 'DSC_REPLICATE')] + tables
        for item in tables:
            fields.append('.'.join(item) if item[1] else item[0])
            if item[1] is None:
                clause.append("'{0}' AS {0}".format(item[0]))
            else:
                idx = [x for x in self.data.keys() if x.lower() == item[0].lower()][0]
                if item[1].lower() not in [x.lower() for x in self.data[idx].keys()]:
                    clause.append("{0}.__output__ AS {0}_DSC_VAR_{1}".\
                                  format(item[0], item[1] if not item[1].startswith('output.') else item[1][7:]))
                else:
                    if item[1] == '__output__':
                        clause.append("{0}.{1} AS {0}_DSC_OUTPUT_".format(item[0], item[1]))
                    else:
                        clause.append("{0}.{1} AS {0}_DSC_FIELD_{1}".format(item[0], item[1]))
        clause = "SELECT " + ', '.join(clause)
        return clause, tables, fields

    @staticmethod
    def match_targets(tables, fields):
        '''
        make sure fields in query do match required targets
        1. Expand query by groups
        2. Check for equality
        '''
        def split(items):
            tb = set()
            fl = set()
            for item in items:
                item = item.split('.')
                tb.add(item[0])
                if len(item) > 1:
                    fl.add(item[1])
            return tb, fl
        #
        targets = [f'{x[0]}.{x[1]}' for x in tables if x[1] != 'DSC_REPLICATE']
        fields = split(fields[1:])
        targets = split(targets)
        if fields[0].issubset(targets[0]) and fields[1] == targets[1]:
            return True
        else:
            return False

    def get_select_clause(self):
        select = []
        select_fields = []
        new_pipelines = []
        for pipeline, first_module in zip(self.pipelines, self.first_modules):
            clause, tables, fields = self.get_one_select_clause(pipeline, first_module)
            if not self.match_targets(tables, fields):
                continue
            new_pipelines.append(pipeline)
            select.append(clause)
            select_fields.append(fields)
        # not all pipelines are useful
        self.pipelines = new_pipelines
        return select, select_fields

    def get_where_clause(self, select_fields):
        return [self.get_one_where_clause(s, list(p)) for s, p in zip(select_fields, self.pipelines)]

    def get_one_where_clause(self, one_select_fields, pipeline_tables):
        '''
        After expanding, condition is a list of list
        the outer lists are connected by OR
        the inner lists are connected by AND
        '''
        select_tables = uniq_list([x.split('.')[0] for x in one_select_fields])
        valid_tables = [x[0] for x in self.condition_tables if x[0] in select_tables + pipeline_tables]
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
                valid_idx = [idx for idx, vv in enumerate(value) if vv[1][0] in valid_tables]
                if len(valid_idx) >= 1:
                    value = ' OR '.join([f"{value[i][0]} ({'.'.join(value[i][1])} {value[i][2]} {value[i][3]})" if len(value[i][0]) else f"{'.'.join(value[i][1])} {value[i][2]} {value[i][3]}" for i in valid_idx])
                    if len(valid_idx) > 1:
                        tmp.append(f"({value})")
                    else:
                        tmp.append(value)
                else:
                    pass
            if len(tmp):
                condition.append(tmp)
        if len(condition):
            return "WHERE " + ' OR '.join(['(' + ' AND '.join([f"({y})" for y in x]) + ')' for x in condition])
        else:
            return ''

    @staticmethod
    def adjust_table(table, ordering = None):
        if len(table) == 0:
            return None
        table = pd.DataFrame(table)
        rename = dict()
        for x in table:
            org = x
            if '_DSC_VAR_' in x:
                x = x.replace('_DSC_VAR_', '.') + ":output"
            if '_DSC_FIELD_' in x:
                x = x.replace('_DSC_FIELD_', '.')
            if '_DSC_OUTPUT_' in x:
                x = x.replace('_DSC_OUTPUT_', '.output.file')
            if org != x:
                rename[org] = x
        if ordering is None:
            table = table[sorted([x for x in table if not "_DSC_VAR_" in x]) + \
                          sorted([x for x in table if "_DSC_VAR_" in x])].rename(columns = rename)
        else:
            table = table[sorted(table.columns, key = lambda x: find_partial_index(x, ordering))].rename(columns = rename)
        return table

    def merge_tables(self):
        common_keys = [t.columns for t in self.output_tables.values()]
        common_keys = list(set(common_keys[0]).intersection(*common_keys))
        table = pd.concat(self.output_tables.values(), join = 'outer', ignore_index = True)
        to_drop = []
        targets = uniq_list([x.split('.', 1)[0] for x in self.targets])
        for g in self.groups:
            if g not in targets:
                continue
            # For each group, find common fields to merge
            # FIXME: the continue / break / reorder logic works here,
            # but can possibly be optimized
            to_merge = dict()
            ordered_group = []
            for col in table.columns:
                for k in self.groups[g]:
                    if not col.startswith(k + '.'):
                        continue
                    if not k in ordered_group:
                        ordered_group.append(k)
                    k = col[len(k):]
                    if not k in to_merge:
                        to_merge[k] = []
                    to_merge[k].append(col)
                    break
            self.groups[g] = ordered_group
            for k in to_merge:
                if len(to_merge[k]) > 1:
                    table[f'{g}{k}'] = table.loc[:, to_merge[k]].apply(tuple, 1)
                    non_na_idx = table[f'{g}{k}'].apply(lambda x: tuple([idx for idx, y in enumerate(x) if y == y]))
                    if not all([len(x) <= 1 for x in non_na_idx]):
                        raise DBError(f'Modules ``{to_merge[k]}`` cannot be grouped into ``{g}{k}`` due to collating entries.')
                    table[f'{g}{k}'] = table[f'{g}{k}'].apply(lambda x: [y for y in x if y == y][0] if len([y for y in x if y == y]) else "NA")
                    if not g in table:
                        table[g] = [self.groups[g][kk[0]] if len(kk) else "NA" for kk in non_na_idx]
                else:
                    # simply rename it
                    table[f'{g}{k}'] = table[to_merge[k][0]]
                    table[g] = [kk for kk in self.groups[g] if to_merge[k][0].startswith(kk + '.')][0]
            to_drop.extend(to_merge.values())
        #
        table.drop(set(sum(to_drop, [])), axis=1, inplace=True)
        # Adjust column name / ordering
        targets = flatten_list([[x] + self.groups[x] if x in self.groups else x for x in targets])
        table = table.rename(columns = {g: f'{g}:id' for g in self.groups})
        table = table[sorted(table.columns, key = lambda x: (find_partial_index(x, targets), not x.endswith(':id')))]
        table = table.rename(columns = {f'{g}:id': g for g in self.groups})
        # Finally deal with the `DSC_REPLICATE` column
        rep_cols = [x for x in table.columns if x.endswith('.DSC_REPLICATE')]
        table.insert(0, 'DSC', table.loc[:, rep_cols].apply(lambda x: tuple(x.dropna().tolist()), 1))
        if not all(table['DSC'].apply(len) == 1):
            raise DBError(f'(Possible bug) DSC replicates cannot be merged due to collating entries.')
        table['DSC'] = table['DSC'].apply(lambda x: int(x[0]))
        table.drop(columns = rep_cols, inplace = True)
        return table

    def fillna(self):
        self.output_table.fillna('NA', inplace = True)
        for k in self.output_tables:
            self.output_tables[k].fillna('NA', inplace = True)
        # now in some situations, eg methods fail systematically,
        # or groups completely non-overlapping, that might result in
        # creating blocks of missing structure.
        # We should consolidate them
        self.output_table.replace('NA', np.nan, inplace = True)
        self.output_table = self.output_table.groupby(self.output_table.columns[self.output_table.notnull().all()].tolist(),
                                  as_index=False).first().fillna('NA')[self.output_table.columns]

    def get_queries(self):
        return self.queries

    def get_data(self):
        return self.data

    def run_queries(self):
        if len(self.queries) == 0:
            raise DBError("Incompatible targets ``{}``{}".\
                          format(', '.join(self.targets),
                                 f' under condition ``{" AND ".join(["(%s)" % x for x in self.raw_condition])}``' if self.raw_condition is not None else ''))
        res = [('+'.join(pipeline), self.adjust_table(sqldf(query, self.data), pipeline)) \
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
