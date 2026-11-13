"""Prove Decimal128 capacity from an immutable Parquet snapshot and bound IR.

Precision in seekdb's aggregate result type is a worst-case SQL type, not the
number of digits in this snapshot. Narrow only after a conservative magnitude
and join-cardinality proof. All arithmetic in this analysis uses rational values.
"""
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction

from parquet_statistics import table_statistics


@dataclass
class Value:
    magnitude: Fraction | None
    spec: object


@dataclass
class Relation:
    rows: int
    values: list
    keys: list


def decimal(spec):
    return isinstance(spec, list) and len(spec) == 3 and spec[0] == 'decimal'


def quantum(spec):
    return Fraction(1, 10**spec[2]) if decimal(spec) else Fraction(1)


def type_bound(spec):
    if decimal(spec):
        return Fraction(10**spec[1]-1, 10**spec[2])
    if spec in ('i8', 'i16', 'i32', 'i64'):
        return Fraction(2**(int(spec[1:])-1))
    if spec == 'bool':
        return Fraction(1)
    return None


def maximum(values):
    return None if any(v is None for v in values) else max(values, default=Fraction(0))


def exact_operand(node):
    # seekdb may upscale a division operand (e.g. DECIMAL(20,6) to (34,20)).
    # It adds zero coefficient digits but does not change the value. Removing
    # only injective widening casts avoids unnecessary 128-bit intermediates.
    while 'cast' in node:
        target, source = node.get('type'), node['cast'].get('type')
        if not (decimal(target) and decimal(source) and target[2] >= source[2]
                and target[1]-target[2] >= source[1]-source[2]):
            break
        node = node['cast']
    return node


class Bounds:
    def __init__(self, tables, parameters):
        self.tables, self.parameters = tables, parameters
        self.sources, self.references = {}, []
        self.nodes = 0

    def source(self, name):
        if name in self.sources:
            return self.sources[name]
        self.sources[name] = table_statistics(self.tables[name])
        return self.sources[name]

    def typed(self, node, bound):
        spec = node.get('type')
        if decimal(spec) and spec[1] > 38:
            if not 0 <= spec[2] <= 38 or bound is None or bound * 10**spec[2] >= 10**38:
                raise ValueError(f'Decimal128 range cannot be proved for {spec}; magnitude bound={bound}')
            node['type'] = ['decimal', 38, spec[2]]
        return Value(bound, node.get('type'))

    def expr(self, node, values, depth=0):
        if depth > 128:
            raise ValueError('Decimal expression proof exceeds depth limit')
        if 'field' in node:
            index = node['field']
            if not isinstance(index, int) or not 0 <= index < len(values):
                raise ValueError('Native field reference outside input')
            return self.typed(node, values[index].magnitude)
        if 'literal' in node or 'param' in node:
            raw = self.parameters[node['param']] if 'param' in node else node['literal']
            spec = node.get('type')
            if raw is None:
                bound = Fraction(0)
            elif decimal(spec) or spec in ('number','i8','i16','i32','i64','bool'):
                value = Decimal(str(int(raw) if isinstance(raw,bool) else raw))
                bound = abs(Fraction(value))
                if spec == 'number':
                    scale = max(0, -value.as_tuple().exponent)
                    precision = max(1, scale, len(value.as_tuple().digits) + max(0, value.as_tuple().exponent))
                    return Value(bound, ['decimal', precision, scale])
            else:
                bound = None
            return self.typed(node, bound)
        if 'cast' in node:
            value = self.expr(node['cast'], values, depth+1)
            bound = value.magnitude
            if bound is not None and decimal(node['type']) and decimal(value.spec) and node['type'][2] < value.spec[2]:
                bound += quantum(node['type'])/2
            return self.typed(node, bound)
        if 'case' in node:
            bounds = []
            for branch in node['case']:
                self.expr(branch['if'], values, depth+1)
                bounds.append(self.expr(branch['then'], values, depth+1).magnitude)
            bounds.append(self.expr(node['else'],values,depth+1).magnitude)
            return self.typed(node, maximum(bounds))
        if 'in' in node:
            self.expr(node['in'],values,depth+1)
            for option in node['values']:
                self.expr(option,values,depth+1)
            return self.typed(node,Fraction(1))
        if 'fn' in node:
            args = []
            if node['fn'] == 'divide' and decimal(node.get('type')):
                for index, original in enumerate(node['args']):
                    candidate = exact_operand(original)
                    value = self.expr(candidate,values,depth+1)
                    limits, cast = [], original
                    while cast is not candidate:
                        limits.append(type_bound(cast['type']))
                        cast = cast['cast']
                    # NUMBER expression precision can be only a planning hint.
                    # Check actual snapshot bounds as well as declared types so
                    # eliminating a cast cannot bypass its range restriction.
                    if limits and (value.magnitude is None or value.magnitude > min(limits)):
                        value = self.expr(original,values,depth+1)
                    else:
                        node['args'][index] = candidate
                    args.append(value)
            else:
                args = [self.expr(arg,values,depth+1) for arg in node['args']]
            bounds = [v.magnitude for v in args]
            name = node['fn']
            bound = type_bound(node.get('type'))
            if name in ('add','subtract','multiply','divide'):
                if any(v is None for v in bounds):
                    bound = None
                elif name in ('add','subtract'):
                    bound = sum(bounds)
                elif name == 'multiply':
                    bound = bounds[0]*bounds[1]
                else:
                    # A nonzero exact denominator is at least one coefficient
                    # unit. Zero denominators become NULL, never infinity.
                    bound = bounds[0]/quantum(args[1].spec)+quantum(node['type'])/2
                    if decimal(node.get('type')):
                        ns = args[0].spec[2] if decimal(args[0].spec) else 0
                        ds = args[1].spec[2] if decimal(args[1].spec) else 0
                        scale = node['type'][2]
                        power = max(ns, scale+ds)
                        # The exact lowering first removes each decimal scale,
                        # then aligns coefficients for integer quotient/remainder.
                        # Bound both the initial Decimal multiplication and the
                        # subsequent signed 128-bit products before execution.
                        products = (bounds[0]*10**(2*ns), bounds[1]*10**(2*ds),
                                    bounds[0]*10**power, bounds[1]*10**(power-scale))
                        if any(product >= 10**38 for product in products):
                            raise ValueError('Decimal division intermediate exceeds Decimal128 range')
            return self.typed(node,bound)
        raise ValueError('Unsupported expression in Decimal128 range proof')

    def measure(self, node, source):
        args = [self.expr(arg, source.values) for arg in node['args']]
        name = node['fn']
        if name == 'count':
            bound = Fraction(source.rows)
        elif name == 'sum':
            bound = None if args[0].magnitude is None else source.rows*args[0].magnitude
        elif name in ('min','max','avg','first'):
            bound = args[0].magnitude
        else:
            bound = None
        return self.typed(node, bound)

    def relation(self, node, depth=0):
        self.nodes += 1
        if depth > 128 or self.nodes > 4096:
            raise ValueError('Decimal relation proof exceeds bounds')
        op = node['op']
        if op == 'reference':
            index = node['id']
            if not isinstance(index,int) or not 0 <= index < len(self.references):
                raise ValueError('Invalid native materialized reference')
            return self.references[index]
        if op == 'read':
            name = (node.get('database','')+'.' if node.get('database') else '')+node['table']
            rows,bounds = self.source(name)
            values = [Value(bounds.get(column) if bounds.get(column) is not None else type_bound(spec),spec)
                      for column,spec in zip(node['columns'],node['types'])]
            keys = [frozenset(key) for key in node.get('unique_keys',[])]
            if any(not key or any(not isinstance(i,int) or not 0 <= i < len(values) for i in key) for key in keys):
                raise ValueError('Invalid native scan key')
            return Relation(rows,values,keys)
        if op == 'join':
            left,right = self.relation(node['left'],depth+1),self.relation(node['right'],depth+1)
            width = len(left.values)
            lkeys,rkeys = set(),set()
            def equality(expr):
                if expr.get('fn') == 'and':
                    for arg in expr['args']: equality(arg)
                if expr.get('fn') == 'equal':
                    a,b = expr['args']
                    if 'field' in a and 'field' in b:
                        a,b = sorted((a['field'],b['field']))
                        if a < width <= b:
                            lkeys.add(a);rkeys.add(b-width)
            for condition in node['conditions']:
                equality(condition)
                self.expr(condition,left.values+right.values)
            lu = any(key <= lkeys for key in left.keys)
            ru = any(key <= rkeys for key in right.keys)
            kind = node['kind']
            if kind in ('left_semi','left_anti'): return left
            if kind in ('right_semi','right_anti'): return right
            rows = left.rows*right.rows
            if ru: rows = min(rows,left.rows)
            if lu: rows = min(rows,right.rows)
            if kind == 'left': rows = max(rows,left.rows)
            if kind == 'right': rows = max(rows,right.rows)
            if kind == 'outer': rows += left.rows+right.rows
            keys = (left.keys if ru and kind in ('inner','left') else []) + [
                frozenset(width+i for i in key) for key in right.keys if lu and kind in ('inner','right')]
            return Relation(rows,left.values+right.values,keys)
        child = self.relation(node['input'],depth+1)
        if op == 'project':
            values = [self.expr(expr,child.values) for expr in node['expressions']]
            mapping = {}
            for i,expr in enumerate(node['expressions']):
                if 'field' in expr: mapping.setdefault(expr['field'],i)
            keys = [frozenset(mapping[i] for i in key) for key in child.keys if key <= mapping.keys()]
            return Relation(child.rows,values,keys)
        if op == 'filter': self.expr(node['condition'],child.values)
        elif op == 'sort':
            for key in node['keys']: self.expr(key['expr'],child.values)
        elif op == 'fetch':
            if node.get('count') is not None:
                count = self.expr(node['count'],[]).magnitude
                if count is not None: child = Relation(min(child.rows,int(count)),child.values,child.keys)
            if node.get('offset') is not None: self.expr(node['offset'],[])
        elif op in ('aggregate','window_aggregate'):
            groups = [self.expr(expr,child.values) for expr in node['groups']]
            measures = [self.measure(expr,child) for expr in node['measures']]
            if op == 'window_aggregate': return Relation(child.rows,child.values+measures,child.keys)
            return Relation(child.rows if groups else 1,groups+measures,[frozenset(range(len(groups)))])
        else:
            raise ValueError('Unsupported relation in Decimal128 range proof: '+op)
        return child


def narrow_decimals(document, parameters, tables):
    bounds = Bounds(tables,parameters)
    for producer in document.get('materialized',[]):
        bounds.references.append(bounds.relation(producer))
    bounds.relation(document['root'])
