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

# keywords for SQLite
# https://www.sqlite.org/lang_keywords.html
SQLITE_KEYWORDS = set([
    'ABORT', 'ACTION', 'ADD', 'AFTER', 'ALL', 'ALTER', 'ANALYZE', 'AND', 'AS',
    'ASC', 'ATTACH', 'AUTOINCREMENT', 'BEFORE', 'BEGIN', 'BETWEEN', 'BY',
    'CASCADE', 'CASE', 'CAST', 'CHECK', 'COLLATE', 'COLUMN', 'COMMIT',
    'CONFLICT', 'CONSTRAINT', 'CREATE', 'CROSS', 'CURRENT', 'CURRENT_DATE',
    'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'DATABASE', 'DEFAULT', 'DEFERRABLE',
    'DEFERRED', 'DELETE', 'DESC', 'DETACH', 'DISTINCT', 'DO', 'DROP', 'EACH',
    'ELSE', 'END', 'ESCAPE', 'EXCEPT', 'EXCLUSIVE', 'EXISTS', 'EXPLAIN',
    'FAIL', 'FILTER', 'FOLLOWING', 'FOR', 'FOREIGN', 'FROM', 'FULL', 'GLOB',
    'GROUP', 'HAVING', 'IF', 'IGNORE', 'IMMEDIATE', 'IN', 'INDEX', 'INDEXED',
    'INITIALLY', 'INNER', 'INSERT', 'INSTEAD', 'INTERSECT', 'INTO', 'IS',
    'ISNULL', 'JOIN', 'KEY', 'LEFT', 'LIKE', 'LIMIT', 'MATCH', 'NATURAL', 'NO',
    'NOT', 'NOTHING', 'NOTNULL', 'NULL', 'OF', 'OFFSET', 'ON', 'OR', 'ORDER',
    'OUTER', 'OVER', 'PARTITION', 'PLAN', 'PRAGMA', 'PRECEDING', 'PRIMARY',
    'QUERY', 'RAISE', 'RANGE', 'RECURSIVE', 'REFERENCES', 'REGEXP', 'REINDEX',
    'RELEASE', 'RENAME', 'REPLACE', 'RESTRICT', 'RIGHT', 'ROLLBACK', 'ROW',
    'ROWS', 'SAVEPOINT', 'SELECT', 'SET', 'TABLE', 'TEMP', 'TEMPORARY', 'THEN',
    'TO', 'TRANSACTION', 'TRIGGER', 'UNBOUNDED', 'UNION', 'UNIQUE', 'UPDATE',
    'USING', 'VACUUM', 'VALUES', 'VIEW', 'VIRTUAL', 'WHEN', 'WHERE', 'WINDOW',
    'WITH', 'WITHOUT'
])

NA = None


def find_partial_index(xx, ordering):
    for ii, i in enumerate(ordering):
        if xx.startswith(i):
            return ii
    if xx.split('.')[1] == 'DSC_REPLICATE':
        return -1
    raise ValueError(f'{xx} not in list {ordering}')


class Query_Processor:
    def __init__(self, db, targets, condition=None, groups=None):
        self.db = db
        self.targets = uniq_list(' '.join(targets).split())
        self.raw_condition = condition
        with open(os.path.expanduser(db), 'rb') as f:
            self.data = pickle.load(f)
        # table: msg map
        self.field_warnings = {}
        if '.groups' in self.data:
            self.groups = self.data['.groups']
        else:
            self.groups = dict()
        if '.depends' in self.data:
            self.depends = dict([
                (k, uniq_list(flatten_list(self.data['.depends'][k])))
                for k in self.data['.depends']
            ])
        else:
            self.depends = None
        # https://github.com/stephenslab/dsc/issues/202
        self.output_checklist = dict(valid={}, invalid={})
        # 1. Check overlapping groups and fix the case when some module in the group has some parameter but others do not
        # changes will be applied to self.data
        self.groups.update(self.get_grouped_tables(groups))
        self.check_overlapping_groups()
        self.add_na_group_parameters()
        # 2. Get query targets and conditions
        self.target_tables = self.get_table_fields(self.targets)
        self.check_output_variables()
        self.condition, self.condition_tables = parse_filter(
            condition, groups=self.groups)
        # 3. only keep tables that do exist in database
        self.target_tables = self.filter_tables(self.target_tables)
        self.condition_tables = self.filter_tables(self.condition_tables)
        # 4. identify and extract which part of each pipeline are involved
        # based on tables in target / condition
        # input pipelines (from data) are:
        # [('rnorm', 'mean', 'MSE'), ('rnorm', 'median', 'MSE'), ... ('rt', 'winsor', 'MSE')]
        self.pipelines, self.target_tables, self.condition_tables = self.filter_pipelines(
            self.data['.pipelines'])
        # 5. make select / from / where clause
        select_clauses = self.get_select_clause()
        from_clauses = self.get_from_clause()
        where_clauses = self.get_where_clause()
        self.queries = uniq_list([
            ' '.join(x)
            for x in list(zip(*[select_clauses, from_clauses, where_clauses]))
        ])
        # 6. run queries
        self.output_tables = self.run_queries()
        # 7. merge table
        self.output_table = self.merge_tables()
        # 8. fillna
        self.fillna()
        # 9. finally show warnings
        self.warn()

    @staticmethod
    def legalize_name(name, kw=False):
        # FIXME: have to ensure keywords conflict is supported
        if name is None:
            return name
        output = ''
        for x in name:
            if re.match(r'^[a-zA-Z0-9_]+$', x):
                output += x
            else:
                output += '_'
        if re.match(r'^[0-9][a-zA-Z0-9_]+$',
                    output) or (output.upper() in SQLITE_KEYWORDS and kw):
            output = '_' + output
        return output

    def check_table_field(self, value, check_field=0):
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
            raise DBError(
                f"``{x}`` does not define a module or a group of modules in current DSC benchmark."
            )
        if y == 'DSC_TIME':
            return
        k = list(self.data.keys())[keys_lower.index(x.lower())]
        y_low = y.lower()
        if y_low == 'dsc_replicate':
            raise DBError(
                f'Cannot query on ``DSC_REPLICATE`` in module ``{k}``')
        if y_low in [i.lower() for i in self.data[k]] and y_low in [
                i.lower() for i in self.data['.output'][k]
        ] and check_field == 1:
            self.field_warnings[
                k] = f"Variable ``{y}`` is both parameter and output in module ``{k}``. Parameter variable ``{y}`` is extracted. To obtain output variable ``{y}`` please use ``{k}.output.{y}`` to specify the query target."
        if not y_low in [i.lower() for i in self.data[k]] and check_field == 2:
            raise DBError(f"Cannot find column ``{y}`` in table ``{k}``")
        if y_low.startswith('output.'):
            y_low = y_low[7:]
        if check_field == 1:
            if y_low not in [i.lower() for i in self.data[k]] and y_low not in [
                i.lower() for i in self.data['.output'][k]]:
                try:
                    self.output_checklist['invalid'][y].append(k)
                except Exception:
                    self.output_checklist['invalid'][y] = [k]
            else:
                try:
                    self.output_checklist['valid'][y].append(k)
                except Exception:
                    self.output_checklist['valid'][y] = [k]
        return

    def check_output_variables(self):
        for k in self.output_checklist['invalid']:
            if k not in self.output_checklist['valid']:
                raise DBError(f"Cannot find variable ``{k}`` in module ``{', '.join(self.output_checklist['invalid'][k])}``")
            # check if the variable is in the same group
            # eg, {'valid': {'alpha': ['elastic_net'], 'beta': ['ridge', 'elastic_net']}, 'invalid': {'alpha': ['ridge']}}
            # is okay because of group {'fit': ['ridge', 'elastic_net']}
            for i in self.output_checklist['invalid'][k]:
                is_valid = []
                for j in self.output_checklist['valid'][k]:
                    is_valid.extend([set([i,j]).issubset(set(s)) for g,s in self.groups.items()])
                if not any(is_valid):
                    raise DBError(f"Cannot find variable ``{k}`` in module ``{i}``")
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
                raise FormatError(
                    f"Invalid module group option ``{g}``. Please use format ``group: module1, module2``"
                )
            g = tuple(x.strip() for x in g.split(':'))
            v = uniq_list([
                x.strip() for x in re.split(r',\s+|\s+|,', g[1]) if x.strip()
            ])
            if g[0] in v:
                raise FormatError(
                    f"Invalid group option: module group name ``{g[0]}``conflicts with module name ``{g[0]}``."
                )
            res[g[0]] = v
        return res

    def check_overlapping_groups(self):
        # for between groups
        for k in list(self.groups.keys()):
            if len(self.groups[k]) == 0:
                del self.groups[k]
        for i, k1 in enumerate(self.groups.keys()):
            for j, k2 in enumerate(self.groups.keys()):
                if i > j:
                    overlap = set(self.groups[k1]).intersection(
                        set(self.groups[k2]))
                    if len(overlap):
                        raise DBError(
                            f"Overlapping groups ``{k1}: {', '.join(self.groups[k1])}`` and ``{k2}: {', '.join(self.groups[k2])}`` is not allowed! You should drop the one that causes the conflict, or use, eg, -g \"{k1}:\" to erase the other one if it is build-in."
                        )
        # for mixing up group and modules in the group
        # FIXME: only check it in targets not conditions
        # possibly a wontfix
        targets = [x.split('.')[0] for x in self.targets]
        modules = [x for x in targets if x not in self.groups]
        groups = [x for x in targets if x in self.groups]
        modules_in_groups = flatten_list([self.groups[k] for k in groups])
        for item in modules:
            if item in modules_in_groups:
                for k in self.groups:
                    if item in self.groups[k]:
                        raise DBError(
                            f"Query targets cannot involve both ``{item}`` and ``{k}``, i.e., a module and a group containing that module."
                        )

    def add_na_group_parameters(self):
        if len(self.groups) == 0:
            return
        for group in list(self.groups.keys()):
            params = uniq_list(
                flatten_list([
                    self.data[item].columns.tolist()
                    for item in self.groups[group] if item in self.data
                ]))
            if len(params) == 0:
                # group is not used
                del self.groups[group]
                continue
            params = [
                x for x in params if x not in
                ['__id__', '__parent__', '__output__', 'DSC_REPLICATE']
            ]
            for param in params:
                for module in self.groups[group]:
                    if module not in self.data:
                        continue
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
            if re.search('^\w+\.\w+$', item) or re.search(
                    '^\w+\.output.\w+$', item):
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
        return uniq_list([
            x for x in tables if x[0].lower() in
            [y.lower() for y in self.data.keys() if not y.startswith('.')]
        ])

    def filter_pipelines(self, pipelines):
        '''
        for each pipeline extract the sub pipeline that the query involves
        '''
        def get_sequence(primary, reference, warnings):
            '''
            tracing back dependencies
            eg, input is primary = ['mnm_identity'], reference = ['oracle_generator', 'small_data', 'identity', 'mnm_identity']
            output is ['mnm_identity', 'identity', 'small_data'] because small_data provides DSC_REPLICATE and oracle_generator is no longer needed.
            '''
            reference = list(reversed(reference))
            primary = sorted(case_insensitive_uniq_list(primary),
                             key=lambda x: reference.index(x))
            while True:
                previous_primary = primary
                for item in primary:
                    if self.depends is not None:
                        depends = [
                            d for d in self.depends[item] if d in reference
                        ]
                    else:
                        depends = [reference[reference.index(item) + 1]] if (
                            reference.index(item) < len(reference) -
                            1) else []
                    if len(depends) > 0:
                        # there is a dependency, let's see if it is already asked for in query targets
                        existing_dependents = [
                            x for x in depends if x in primary
                        ]
                        if len(existing_dependents) == len(depends):
                            continue
                        # there are additional dependencies not yet in query targets
                        # we need to get them, by grabing the most downstream one.
                        # I think it should be enough?
                        depend_step = reference[min([
                            reference.index(dd) for dd in depends
                            if dd not in existing_dependents
                        ])]
                        if depend_step not in primary:
                            primary.append(depend_step)
                primary = sorted(case_insensitive_uniq_list(primary),
                                 key=lambda x: reference.index(x))
                if primary == previous_primary:
                    break
            # a sequence can lose dependency half-way
            # in which case an warning message will be given
            idx = 0
            while idx < (len(primary) - 1):
                item = primary[idx]
                if self.depends is not None and primary[
                        idx + 1] not in self.depends[item]:
                    warnings.append(
                        f'Requested/intermediate module ``{primary[idx+1]}`` is not connected to module ``{item}``; thus removed from sub-query involving module ``{item}``.'
                    )
                    del primary[idx + 1]
                    idx -= 1
                idx += 1
            return primary

        #
        valid_tables = [[
            item[0] for item in self.target_tables + self.condition_tables
            if item[0] in pipeline
        ] for pipeline in pipelines]
        # 1. Further filter pipelines to minimally match target table dependencies
        # 2. For pipelines containing each other we only keep the longest pipelines
        warnings = []
        long_pipelines = filter_sublist([
            get_sequence(tables, pipeline, warnings)
            for tables, pipeline in zip(valid_tables, pipelines)
        ])
        if len(warnings):
            for item in uniq_list(warnings):
                logger.warning(item)
        target_tables = [[
            item for item in self.target_tables if item[0] in pipeline
        ] for pipeline in long_pipelines]
        condition_tables = [[
            item for item in self.condition_tables if item[0] in pipeline
        ] for pipeline in long_pipelines]
        non_empty_targets = [
            idx for idx, item in enumerate(target_tables) if len(item) > 0
        ]
        return [long_pipelines[i] for i in non_empty_targets
                ], [target_tables[i] for i in non_empty_targets
                    ], [condition_tables[i] for i in non_empty_targets]

    def get_from_clause(self):
        res = [f'FROM "{sequence[0]}" ' + ' '.join(['INNER JOIN "{1}" ON "{0}".__parent__ = "{1}".__id__'.format(sequence[i], sequence[i+1]) for i in range(len(sequence) - 1)]).strip() \
             for sequence in self.pipelines]
        return res

    def get_one_select_clause(self, pipeline, tables):
        clause = []
        fields = []
        tables = [(pipeline[-1], 'DSC_REPLICATE')] + tables
        for item in tables:
            fields.append('.'.join(item) if item[1] else item[0])
            if item[1] is None:
                clause.append("'{0}' AS {0}".format(item[0]))
            else:
                idx = [
                    x for x in self.data.keys()
                    if x.lower() == item[0].lower()
                ][0]
                if item[1].lower() not in [
                        x.lower() for x in self.data[idx].keys()
                ]:
                    clause.append('"{0}".__output__ AS {0}_DSC_VAR_{1}'.\
                                  format(item[0], item[1] if not item[1].startswith('output.') else item[1][7:]))
                else:
                    if item[1] == '__output__':
                        clause.append('"{0}".{1} AS {0}_DSC_OUTPUT_'.format(
                            item[0], item[1]))
                    else:
                        clause.append('"{0}".{1} AS {0}_DSC_FIELD_{1}'.format(
                            item[0], item[1]))
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
                # if item[1] == 'DSC_REPLICATE'
                #    continue
                tb.add(item[0])
                if len(item) > 1:
                    fl.add(item[1])
            return tb, fl

        #
        targets = [f'{x[0]}.{x[1]}' for x in tables]
        fields = split(fields)
        targets = split(targets)
        if fields[0].issubset(targets[0]) and fields[1] == targets[1]:
            return True
        else:
            return False

    def get_select_clause(self):
        select = []
        for pipeline, tables in zip(self.pipelines, self.target_tables):
            clause, tables, fields = self.get_one_select_clause(
                pipeline, tables)
            if not self.match_targets(tables, fields):
                continue
            select.append(clause)
        return select

    def get_where_clause(self):
        return [
            self.get_one_where_clause(t, c, p) for t, c, p in zip(
                self.target_tables, self.condition_tables, self.pipelines)
        ]

    def get_one_where_clause(self, target_tables, condition_tables, pipeline):
        '''
        After expanding, condition is a list of list
        the outer lists are connected by OR
        the inner lists are connected by AND
        '''
        select_tables = case_insensitive_uniq_list(
            [x[0] for x in target_tables])
        valid_tables = [
            x[0] for x in condition_tables if x[0] in select_tables + pipeline
        ]
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
                valid_idx = [
                    idx for idx, vv in enumerate(value)
                    if vv[1][0] in valid_tables
                ]
                if len(valid_idx) >= 1:
                    value = ' OR '.join([
                        f'{value[i][0]} ("{value[i][1][0]}".{value[i][1][1]} {value[i][2]} {value[i][3]})'
                        if len(value[i][0]) else
                        f'"{value[i][1][0]}".{value[i][1][1]} {value[i][2]} {value[i][3]}'
                        for i in valid_idx
                    ])
                    if len(valid_idx) > 1:
                        tmp.append(f"({value})")
                    else:
                        tmp.append(value)
                else:
                    pass
            if len(tmp):
                condition.append(tmp)
        if len(condition):
            return "WHERE " + ' OR '.join([
                '(' + ' AND '.join([f"({y})" for y in x]) + ')'
                for x in condition
            ])
        else:
            return ''

    @staticmethod
    def adjust_table(table, ordering=None):
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
            table = table[sorted([x for x in table if "_DSC_VAR_" not in x]) + \
                          sorted([x for x in table if "_DSC_VAR_" in x])].rename(columns = rename)
        else:
            table = table[sorted(
                table.columns,
                key=lambda x: find_partial_index(x, ordering))].rename(
                    columns=rename)
        return table

    def merge_tables(self):
        common_keys = [t.columns for t in self.output_tables.values()]
        common_keys = list(set(common_keys[0]).intersection(*common_keys))
        table = pd.concat(self.output_tables.values(),
                          join='outer',
                          ignore_index=True,
                          sort=False)
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
                    if k not in ordered_group:
                        ordered_group.append(k)
                    k = col[len(k):]
                    if k not in to_merge:
                        to_merge[k] = []
                    to_merge[k].append(col)
                    break
            self.groups[g] = ordered_group
            # handle non-trivial groups first
            to_merge = dict(
                sorted(to_merge.items(),
                       key=lambda kv: (len(kv[1]), kv[0]),
                       reverse=True))
            for k in to_merge:
                if len(ordered_group) > 1:
                    table[f'{g}{k}'] = table.loc[:,
                                                 to_merge[k]].apply(tuple, 1)
                    non_na_idx = table[f'{g}{k}'].apply(lambda x: tuple(
                        [idx for idx, y in enumerate(x) if y == y]))
                    if not all([len(x) <= 1 for x in non_na_idx]):
                        raise DBError(
                            f'Modules ``{to_merge[k]}`` cannot be grouped into ``{g}{k}`` due to collating entries.'
                        )
                    table[f'{g}{k}'] = table[f'{g}{k}'].apply(
                        lambda x: [y for y in x if y == y][0]
                        if len([y for y in x if y == y]) else NA)
                    if g not in table:
                        table[g] = [
                            self.groups[g][kk[0]] if len(kk) else NA
                            for kk in non_na_idx
                        ]
                else:
                    # it is a trivial group
                    # simply rename it
                    table[f'{g}{k}'] = table[to_merge[k][0]]
                    table[g] = [
                        kk for kk in self.groups[g]
                        if to_merge[k][0].startswith(kk + '.')
                    ][0]
            to_drop.extend(to_merge.values())
        #
        table.drop(set(sum(to_drop, [])), axis=1, inplace=True)
        # Adjust column name / ordering
        targets = flatten_list([[x] + self.groups[x] if x in self.groups else x
                                for x in targets])
        table = table.rename(columns={g: f'{g}:id' for g in self.groups})
        table = table[sorted(
            table.columns,
            key=lambda x:
            (find_partial_index(x, targets), not x.endswith(':id')))]
        table = table.rename(columns={f'{g}:id': g for g in self.groups})
        # Finally deal with the `DSC_REPLICATE` column
        rep_cols = [x for x in table.columns if x.endswith('.DSC_REPLICATE')]
        table.insert(
            0, 'DSC',
            table.loc[:, rep_cols].apply(lambda x: tuple(x.dropna().tolist()),
                                         1))
        if not all(table['DSC'].apply(len) == 1):
            raise DBError(
                f'(Possible bug) DSC replicates cannot be merged due to collating entries.'
            )
        table['DSC'] = table['DSC'].apply(lambda x: int(x[0]))
        table.drop(columns=rep_cols, inplace=True)
        return table

    def fillna(self):
        self.output_table.fillna('NA', inplace=True)
        for k in self.output_tables:
            self.output_tables[k].fillna('NA', inplace=True)

    def consolidate_subrows(self):
        # situations 1:
        # now in some situations, eg methods fail systematically,
        # or groups completely non-overlapping, that might result in
        # creating blocks of missing structure.
        # We should consolidate them
        ## FIXME: disable this feature because it is not clear whether or not this is good idea
        ## without trying to guess the context (by parameter and value)
        ## see https://github.com/stephenslab/dsc/issues/145
        # self.output_table.replace('NA', np.nan, inplace = True)
        # self.output_table = self.output_table.groupby(self.output_table.columns[self.output_table.notnull().all()].tolist(),
        #                           as_index=False).first().fillna('NA')[self.output_table.columns]
        # situation 2: some rows with NA are exactly subset of some other rows
        # in this case just drop those lines
        pass

    def get_queries(self):
        return self.queries

    def get_data(self):
        return self.data

    def run_queries(self):
        if len(self.queries) == 0:
            raise DBError("Incompatible targets ``{}``{}".\
                          format(', '.join(self.targets),
                                 f' under condition ``{" AND ".join(["(%s)" % x for x in self.raw_condition])}``' if self.raw_condition is not None else ''))
        res = [('+'.join(reversed(pipeline)), self.adjust_table(sqldf(query.strip(), self.data, pipeline), pipeline)) \
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
