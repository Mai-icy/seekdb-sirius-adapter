"""Encode seekdb's bound relational IR as Substrait; no SQL parser or optimizer.

IR v1 is private to the seekdb/adapter boundary. The output is Substrait for
unmodified Sirius main. Unsupported shapes fail before execution.
"""
import json
from dataclasses import dataclass
from decimal import Decimal, localcontext
import math

import pyarrow as pa
from substrait.plan_pb2 import Plan
from substrait.algebra_pb2 import Rel, Expression
from substrait.type_pb2 import Type
from decimal_bounds import narrow_decimals


def data_type(spec):
    result = Type()
    if isinstance(spec, list) and len(spec) == 3 and spec[0] == "decimal":
        _, precision, scale = spec
        if not 1 <= precision <= 38 or not 0 <= scale <= precision:
            raise ValueError(f"Sirius Decimal128 requires precision 1..38 and scale 0..precision; native type is {spec}")
        result.decimal.precision, result.decimal.scale = precision, scale
        result.decimal.nullability = 1
    elif spec in ("bool", "i8", "i16", "i32", "i64", "fp32", "fp64", "string", "date"):
        getattr(result, spec).nullability = 1
    else:
        raise ValueError(f"Unsupported native type: {spec}")
    return result


def arrow_spec(t):
    if pa.types.is_integer(t) and pa.types.is_signed_integer(t):
        return f"i{t.bit_width}"
    if pa.types.is_decimal128(t):
        return ["decimal", t.precision, t.scale]
    for kind, predicate in (("bool", pa.types.is_boolean), ("fp32", pa.types.is_float32),
                            ("fp64", pa.types.is_float64), ("string", pa.types.is_string),
                            ("date", pa.types.is_date32)):
        if predicate(t):
            return kind
    raise ValueError(f"Unsupported native Arrow type: {t}")


@dataclass(frozen=True)
class ParquetTable:
    """Physical file schema and explicit immutable file paths; no row materialization."""
    schema: pa.Schema
    files: tuple[str, ...]
    statistics_path: str | None = None


class Encoder:
    def __init__(self, tables, parameters):
        self.tables, self.parameters = tables, parameters
        self.plan, self.scans, self.functions = Plan(), [], {}
        self.nodes = 0
        self.references = []
        # This adapter targets the pinned consumer, not whatever producer is
        # installed in a Python environment.
        self.plan.version.major_number = 0
        self.plan.version.minor_number = 78
        self.plan.version.producer = "seekdb-native-plan-v1"

    def function(self, name):
        allowed = {"not_equal", "equal", "lt", "lte", "gt", "gte", "and", "or", "not",
                   "add", "subtract", "multiply", "divide", "sum", "count", "min", "max", "avg", "is_null", "is_not_null", "is_not_distinct_from",
                   "rtrim", "regexp_replace", "like_escape", "like", "substring", "year", "extract", "between", "first",
                   "cast_to_type", "factorial", "%", "//"}
        if name not in allowed:
            raise ValueError(f"Unsupported native function: {name}")
        if name not in self.functions:
            anchor = len(self.functions) + 1
            extension = self.plan.extensions.add().extension_function
            extension.function_anchor, extension.name = anchor, name
            self.functions[name] = anchor
        return self.functions[name]

    def literal(self, value, spec):
        out = Expression()
        if spec == "number":
            number = Decimal(value) if value is not None else Decimal(0)
            scale = max(0, -number.as_tuple().exponent)
            precision = max(1, scale, len(number.as_tuple().digits) + max(0, number.as_tuple().exponent))
            spec = ["decimal", precision, scale]
        if value is None:
            out.literal.null.CopyFrom(data_type(spec))
        elif isinstance(spec, list):
            with localcontext() as context:
                context.prec = 80
                scaled = Decimal(value).scaleb(spec[2])
                if not scaled.is_finite() or scaled != scaled.to_integral_value() or abs(scaled) >= 10 ** spec[1]:
                    raise ValueError("Native decimal literal loses precision")
                out.literal.decimal.value = int(scaled).to_bytes(16, "little", signed=True)
                out.literal.decimal.precision, out.literal.decimal.scale = spec[1:]
        elif spec in ("i8", "i16", "i32", "i64", "date"):
            value = int(value)
            bits = 32 if spec == "date" else int(spec[1:])
            if not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
                raise ValueError("Native integer literal out of range")
            setattr(out.literal, spec, value)
        elif spec in ("fp32", "fp64"):
            value = float(value)
            if not math.isfinite(value):
                raise ValueError("Nonfinite native literal")
            setattr(out.literal, spec, value)
        elif spec == "bool":
            if value not in (True, False, "0", "1"):
                raise ValueError("Invalid native boolean literal")
            out.literal.boolean = value in (True, "1")
        elif spec == "string" and isinstance(value, str):
            out.literal.string = value
        else:
            raise ValueError("Unsupported native literal")
        return out

    def decimal_division(self, node, width, depth):
        """Inline the old exact quotient/remainder macro using public functions.

        Substrait's pinned reader has no HUGEINT type mapping. cast_to_type's
        second argument provides only its bind-time type; factorial(0) folds to
        a HUGEINT constant. No custom macro or consumer patch is installed.
        """
        integer = ["decimal", 38, 0]

        def call(name, args, spec=integer):
            out = Expression()
            out.scalar_function.function_reference = self.function(name)
            out.scalar_function.output_type.CopyFrom(data_type(spec))
            for arg in args:
                out.scalar_function.arguments.add().value.CopyFrom(arg)
            return out

        def huge(value):
            return call("cast_to_type", [value, call("factorial", [self.literal(0, "i32")])])

        def choose(condition, yes, no):
            out = Expression()
            branch = out.if_then.ifs.add()
            getattr(branch, "if").CopyFrom(condition)
            branch.then.CopyFrom(yes)
            getattr(out.if_then, "else").CopyFrom(no)
            return out

        def scale(arg):
            spec = arg.get("type")
            if isinstance(spec, list) and spec[0] == "decimal":
                return spec[2]
            if spec in ("i8", "i16", "i32", "i64"):
                return 0
            if spec == "number":
                return self.expr(arg, width, depth + 1).literal.decimal.scale
            raise ValueError("Decimal division requires exact operand types")

        def power(exponent):
            if abs(exponent) >= 38:
                raise ValueError("Decimal division scale exceeds Decimal128")
            return self.literal(str(Decimal(1).scaleb(exponent)),
                                ["decimal", abs(exponent) + 1, max(0, -exponent)])

        numerator, denominator = node["args"]
        ns, ds, result_scale = scale(numerator), scale(denominator), node["type"][2]
        exponent = max(ns, result_scale + ds)
        n = huge(call("multiply", [self.expr(numerator, width, depth + 1), power(ns)]))
        d = huge(call("multiply", [self.expr(denominator, width, depth + 1), power(ds)]))
        n = call("multiply", [n, huge(power(exponent - ns))])
        d = call("multiply", [d, huge(power(exponent - result_scale - ds))])
        zero, one, two = (huge(self.literal(v, "i64")) for v in (0, 1, 2))
        # seekdb SELECT arithmetic returns NULL on a zero denominator.
        d = choose(call("equal", [d, zero], "bool"), huge(self.literal(None, "i64")), d)
        quotient, remainder = call("//", [n, d]), call("%", [n, d])
        nnegative, dnegative = call("lt", [n, zero], "bool"), call("lt", [d, zero], "bool")
        abs_remainder = choose(nnegative, call("subtract", [zero, remainder]), remainder)
        abs_d = choose(dnegative, call("subtract", [zero, d]), d)
        half = call("add", [call("//", [abs_d, two]), call("%", [abs_d, two])])
        sign = choose(call("not_equal", [nnegative, dnegative], "bool"),
                      huge(self.literal(-1, "i64")), one)
        rounded = call("add", [quotient, choose(call("gte", [abs_remainder, half], "bool"), sign, zero)])
        decimal = Expression()
        decimal.cast.input.CopyFrom(rounded)
        decimal.cast.type.CopyFrom(data_type(integer))
        decimal.cast.failure_behavior = 2
        result = call("multiply", [decimal, power(-result_scale)], node["type"])
        out = Expression()
        out.cast.input.CopyFrom(result)
        out.cast.type.CopyFrom(data_type(node["type"]))
        out.cast.failure_behavior = 2
        return out

    def expr(self, node, width, depth=0):
        if depth > 128:
            raise ValueError("Native expression depth exceeds 128")
        out = Expression()
        if "field" in node:
            index = node["field"]
            if not isinstance(index, int) or not 0 <= index < width:
                raise ValueError("Native field reference outside input")
            out.selection.root_reference.SetInParent()
            out.selection.direct_reference.struct_field.field = index
        elif "param" in node or "literal" in node:
            if "param" in node:
                index = node["param"]
                if not isinstance(index, int) or not 0 <= index < len(self.parameters):
                    raise ValueError("Unbound native plan parameter")
                value = self.parameters[index]
            else:
                value = node["literal"]
            out = self.literal(value, node["type"])
        elif "cast" in node:
            source = node["cast"]
            target, original = node["type"], source.get("type")
            if isinstance(target, list) and isinstance(original, list) and original[2] > target[2]:
                # cuDF's decimal rescale truncates. seekdb CAST rounds halves
                # away from zero; apply an exact half-unit before rescaling.
                half = str(Decimal(5).scaleb(-target[2] - 1))
                # DuckDB represents precision <= 4 with INT16, which the GPU
                # Decimal mapper cannot carry. INT32 has the same exact scale.
                half_type = ["decimal", max(5, target[2] + 2), target[2] + 1]
                zero = {"literal": "0", "type": "i64"}
                source = {"fn": "add", "type": original, "args": [source, {
                    "case": [{"if": {"fn": "lt", "type": "bool", "args": [source, zero]},
                              "then": {"literal": "-" + half, "type": half_type}}],
                    "else": {"literal": half, "type": half_type}}]}
            out.cast.input.CopyFrom(self.expr(source, width, depth + 1))
            out.cast.type.CopyFrom(data_type(node["type"]))
            out.cast.failure_behavior = 2  # THROW_EXCEPTION
        elif "case" in node:
            for branch in node["case"]:
                item = out.if_then.ifs.add()
                getattr(item, "if").CopyFrom(self.expr(branch["if"], width, depth + 1))
                item.then.CopyFrom(self.expr(branch["then"], width, depth + 1))
            getattr(out.if_then, "else").CopyFrom(self.expr(node["else"], width, depth + 1))
        elif "in" in node:
            out.singular_or_list.value.CopyFrom(self.expr(node["in"], width, depth + 1))
            for value in node["values"]:
                out.singular_or_list.options.add().CopyFrom(self.expr(value, width, depth + 1))
            if node["negate"]:
                negated = Expression()
                negated.scalar_function.function_reference = self.function("not")
                negated.scalar_function.output_type.CopyFrom(data_type("bool"))
                negated.scalar_function.arguments.add().value.CopyFrom(out)
                out = negated
        elif "fn" in node:
            name, args = node["fn"], node["args"]
            if name == "divide" and isinstance(node["type"], list):
                return self.decimal_division(node, width, depth)
            if name == "rtrim":
                return self.expr({"fn": "regexp_replace", "type": "string", "args": [args[0],
                                  {"literal": " +$", "type": "string"}, {"literal": "", "type": "string"}]}, width, depth + 1)
            if name == "like_escape":
                # Two-argument LIKE suffices only when the bound pattern does
                # not use the escape character. Never silently drop an escape.
                def bound_text(arg):
                    if "cast" in arg and arg["type"] == "string":
                        return bound_text(arg["cast"])
                    value = self.expr(arg, width, depth + 1)
                    if value.HasField("literal") and value.literal.WhichOneof("literal_type") == "string":
                        return value.literal.string
                    raise ValueError("LIKE requires a bound pattern and escape")
                pattern, escape = bound_text(args[1]), bound_text(args[2])
                if escape and escape in pattern:
                    raise ValueError("Escaped LIKE pattern requires GPU escape support")
                name, args = "like", args[:2]
            # The pinned reader misinterprets string not_equal as NOT LIKE.
            # NOT(equal()) preserves literal '%'/'_' comparison semantics.
            if name == "not_equal" and any(arg.get("type") == "string" for arg in args):
                return self.expr({"fn": "not", "type": "bool", "args": [
                    {"fn": "equal", "type": "bool", "args": args}]}, width, depth + 1)
            out.scalar_function.function_reference = self.function(name)
            out.scalar_function.output_type.CopyFrom(data_type(node["type"]))
            for index, arg in enumerate(args):
                if name == "extract" and index == 0:
                    unit = self.expr(arg, width, depth + 1).literal
                    # seekdb ObDateUnitType: YEAR = 8.
                    if unit.WhichOneof("literal_type") not in ("i32", "i64"):
                        raise ValueError("Invalid date unit")
                    value = getattr(unit, unit.WhichOneof("literal_type"))
                    if value != 8:
                        raise ValueError(f"Unsupported date unit {value}")
                    out.scalar_function.arguments.add().enum = "YEAR"
                else:
                    out.scalar_function.arguments.add().value.CopyFrom(self.expr(arg, width, depth + 1))
        else:
            raise ValueError("Unsupported native expression")
        return out

    def conjunction(self, expressions):
        if not expressions:
            return {"literal": True, "type": "bool"}
        result = expressions[0]
        for item in expressions[1:]:
            result = {"fn": "and", "args": [result, item], "type": "bool"}
        return result

    def fetch_bound(self, node, depth=0):
        """Bind optimizer-generated LIMIT + OFFSET without SQL evaluation."""
        if depth > 128:
            raise ValueError("Native fetch expression depth exceeds 128")
        if node.get("fn") in ("add", "subtract") and len(node["args"]) == 2:
            left, right = (self.fetch_bound(arg, depth + 1) for arg in node["args"])
            value = left + right if node["fn"] == "add" else left - right
        else:
            expr = self.expr(node, 0)
            if not expr.HasField("literal"):
                raise ValueError("Native fetch requires a bound integer expression")
            kind = expr.literal.WhichOneof("literal_type")
            if kind not in ("i8", "i16", "i32", "i64"):
                raise ValueError("Native fetch requires an integer constant")
            value = getattr(expr.literal, kind)
        if not 0 <= value < 2**63:
            raise ValueError("Native fetch bound outside signed int64 range")
        return value

    def relation(self, node, depth=0):
        self.nodes += 1
        if depth > 128 or self.nodes > 4096:
            raise ValueError("Native relation tree exceeds bounds")
        out, op = Rel(), node["op"]
        if op == "reference":
            index = node["id"]
            if not isinstance(index, int) or not 0 <= index < len(self.references):
                raise ValueError("Invalid native materialized reference")
            out.reference.subtree_ordinal = index
            return out, self.references[index]
        if op == "read":
            name = node["table"]
            if node.get("database"):
                name = node["database"] + "." + name
            if name not in self.tables or len(self.scans) >= 64:
                raise ValueError(f"Undeclared native input or too many scans: {name}")
            source = self.tables[name]
            original = source.schema
            if not source.files or not len(original) or len(set(original.names)) != len(original):
                raise ValueError("Parquet input requires files and unique column names")
            positions = [original.get_field_index(c) for c in node["columns"]]
            if len(positions) != len(set(positions)) or any(c < 0 for c in positions):
                raise ValueError("Invalid native read columns")
            expected = node.get("types")
            if expected is None or len(expected) != len(positions):
                raise ValueError("Native read requires seekdb column types")
            for position, spec in zip(positions, expected):
                if arrow_spec(original[position].type) != spec:
                    raise ValueError(f"Parquet type differs from seekdb: {name}.{original[position].name}")
            positions = positions or [0]  # Physical column carrying the row count.
            schema = pa.schema([original[i] for i in positions])
            for path in source.files:
                item = out.read.local_files.items.add()
                item.uri_file = path
                item.parquet.SetInParent()
            # Upstream's local-files reader aligns base_schema by name when
            # projection is absent. Never use compact positions against a full file.
            out.read.base_schema.struct.nullability = 2
            for field in schema:
                out.read.base_schema.names.append(field.name)
                out.read.base_schema.struct.types.add().CopyFrom(data_type(arrow_spec(field.type)))
            self.scans.append({"table": name, "columns": schema.names, "files": list(source.files)})
            return out, len(schema)
        if op == "join":
            left, lw = self.relation(node["left"], depth + 1)
            right, rw = self.relation(node["right"], depth + 1)
            kind = node["kind"]
            kinds = {"inner": 1, "outer": 2, "left": 3, "right": 4,
                     "left_semi": 5, "left_anti": 6, "right_semi": 8, "right_anti": 9}
            if kind in ("left_anti", "right_anti"):
                condition = self.expr(self.conjunction(node["conditions"]), lw + rw)
                if kind == "right_anti":
                    # Swap inputs and rebase bound field ordinals, including
                    # selections nested in casts, CASE and scalar functions.
                    def rebase(message):
                        if isinstance(message, Expression) and message.HasField("selection"):
                            field = message.selection.direct_reference.struct_field
                            field.field = rw + field.field if field.field < lw else field.field - lw
                        for descriptor, value in message.ListFields():
                            if descriptor.message_type is not None:
                                for child in value if descriptor.is_repeated else (value,):
                                    rebase(child)
                    rebase(condition)
                    left, right, lw, rw = right, left, rw, lw
                # A non-NULL marker distinguishes an unmatched outer row even
                # when every original right column is nullable. Preserve bag
                # semantics: duplicate matches disappear and unmatched left
                # duplicates survive. Ordinary NULL predicates remain UNKNOWN.
                marked = Rel()
                marked.project.input.CopyFrom(right)
                marked.project.expressions.add().CopyFrom(self.literal(1, "i8"))
                joined = Rel()
                joined.join.left.CopyFrom(left)
                joined.join.right.CopyFrom(marked)
                joined.join.type = 3
                joined.join.expression.CopyFrom(condition)
                filtered = Rel()
                filtered.filter.input.CopyFrom(joined)
                filtered.filter.condition.CopyFrom(self.expr({"fn": "is_null", "type": "bool",
                    "args": [{"field": lw + rw}]}, lw + rw + 1))
                out.project.input.CopyFrom(filtered)
                out.project.common.emit.output_mapping.extend(range(lw))
                return out, lw
            if kind not in kinds:
                raise ValueError("Unsupported native join")
            if kind == "inner" and not node["conditions"]:
                out.cross.left.CopyFrom(left)
                out.cross.right.CopyFrom(right)
            else:
                out.join.left.CopyFrom(left)
                out.join.right.CopyFrom(right)
                out.join.type = kinds[kind]
                out.join.expression.CopyFrom(self.expr(self.conjunction(node["conditions"]), lw + rw))
            return out, lw if kind in ("left_semi", "left_anti") else rw if kind in ("right_semi", "right_anti") else lw + rw
        if op == "aggregate" and any(node.get("string_keys", ())):
            # PAD SPACE grouping uses a canonical key while returning an
            # original representative (trimming the selected value is wrong).
            groups, measures = list(node["groups"]), list(node["measures"])
            mapping = list(range(len(groups) + len(measures)))
            for index, is_text in enumerate(node["string_keys"]):
                if is_text:
                    original = groups[index]
                    groups[index] = {"fn": "rtrim", "args": [original], "type": "string"}
                    mapping[index] = len(groups) + len(measures)
                    measures.append({"fn": "min", "args": [original], "type": "string", "distinct": False})
            return self.relation({"op": "project", "input": {"op": "aggregate", "input": node["input"],
                "groups": groups, "measures": measures}, "expressions": [{"field": i} for i in mapping]}, depth + 1)
        if op == "window_aggregate":
            # Full-frame window = grouped aggregate joined back to the same
            # snapshot. Each occurrence has an independent replayable scan.
            source, groups, measures = node["input"], node["groups"], node["measures"]
            left, width = self.relation(source, depth + 1)
            right, rw = self.relation({"op": "aggregate", "input": source,
                                      "groups": groups, "measures": measures}, depth + 1)
            joined = Rel()
            if groups:
                joined.join.left.CopyFrom(left)
                joined.join.right.CopyFrom(right)
                joined.join.type = 1
                conditions = [{"fn": "is_not_distinct_from", "type": "bool",
                               "args": [key, {"field": width + i}]} for i, key in enumerate(groups)]
                joined.join.expression.CopyFrom(self.expr(self.conjunction(conditions), width + rw))
            else:
                joined.cross.left.CopyFrom(left)
                joined.cross.right.CopyFrom(right)
            out.project.input.CopyFrom(joined)
            out.project.common.emit.output_mapping.extend(list(range(width)) + list(range(width + len(groups), width + rw)))
            return out, width + len(measures)
        child, width = self.relation(node["input"], depth + 1)
        if op == "filter":
            out.filter.input.CopyFrom(child)
            out.filter.condition.CopyFrom(self.expr(node["condition"], width))
        elif op == "project":
            out.project.input.CopyFrom(child)
            for expr in node["expressions"]:
                out.project.expressions.add().CopyFrom(self.expr(expr, width))
            out.project.common.emit.output_mapping.extend(range(width, width + len(node["expressions"])))
            width = len(node["expressions"])
        elif op == "aggregate":
            out.aggregate.input.CopyFrom(child)
            grouping = out.aggregate.groupings.add()
            for index, expr in enumerate(node["groups"]):
                out.aggregate.grouping_expressions.add().CopyFrom(self.expr(expr, width))
                grouping.expression_references.append(index)
            for expr in node["measures"]:
                measure = out.aggregate.measures.add().measure
                measure.function_reference = self.function(expr["fn"])
                measure.output_type.CopyFrom(data_type(expr["type"]))
                measure.phase = 3  # INITIAL_TO_RESULT; no partial aggregation
                measure.invocation = 2 if expr["distinct"] else 1
                for arg in expr["args"]:
                    measure.arguments.add().value.CopyFrom(self.expr(arg, width))
            width = len(node["groups"]) + len(node["measures"])
            # The upstream reader binds aggregate return types itself; in
            # particular SUM(BIGINT) widens to DECIMAL even when the host's
            # measure.output_type is i64 (e.g. seekdb's two-stage COUNT).
            # Preserve the host contract explicitly before any consumer uses it.
            typed = Rel()
            typed.project.input.CopyFrom(out)
            group_count = len(node["groups"])
            for index in range(width):
                value = {"field": index}
                if index >= group_count:
                    value = {"cast": value, "type": node["measures"][index - group_count]["type"]}
                typed.project.expressions.add().CopyFrom(self.expr(value, width))
            typed.project.common.emit.output_mapping.extend(range(width, width * 2))
            out = typed
        elif op == "sort":
            out.sort.input.CopyFrom(child)
            for key in node["keys"]:
                if key["direction"] not in range(4):
                    raise ValueError("Unsupported native sort direction")
                field = out.sort.sorts.add()
                field.expr.CopyFrom(self.expr(key["expr"], width))
                field.direction = key["direction"] + 1
        elif op == "fetch":
            out.fetch.input.CopyFrom(child)
            for key, default in (("count", -1), ("offset", 0)):
                expr = node[key]
                if expr is None:
                    value = default
                else:
                    value = self.fetch_bound(expr)
                setattr(out.fetch, key, value)  # Upstream reader consumes scalar count/offset.
        else:
            raise ValueError(f"Unsupported native relation: {op}")
        return out, width


def encode_plan(template, parameters, tables):
    if not isinstance(template, str) or not 0 < len(template.encode()) <= 16 * 1024**2:
        raise ValueError("Missing or oversized seekdb native plan")
    document = json.loads(template)
    if document.get("version") != 1:
        raise ValueError("Unsupported seekdb native plan version")
    encoder = Encoder(tables, parameters)
    materialized = document.get("materialized", [])
    if not isinstance(materialized, list) or len(materialized) > 64:
        raise ValueError("Invalid native materialized relations")
    def walk(value):
        if isinstance(value, dict):
            yield value
            for child in value.values():
                yield from walk(child)
        elif isinstance(value, list):
            for child in value:
                yield from walk(child)
    if any(isinstance(n.get("type"), list) and n["type"][0] == "decimal"
           and (n["type"][1] > 38 or n.get("fn") == "divide")
           for n in walk(document)):
        narrow_decimals(document, parameters, tables)
    # References can replay immutable Parquet inputs. Unlike the old one-shot
    # Arrow streams, repeated binding cannot consume an input prematurely.
    # Do not promise one physical materialization in the current reader.
    for producer in materialized:
        if any(n.get("op") == "fetch" or n.get("type") in ("fp32", "fp64") for n in walk(producer)):
            raise ValueError("Materialized replay requires deterministic exact expressions")
        relation, width = encoder.relation(producer)
        encoder.plan.relations.add().rel.CopyFrom(relation)
        encoder.references.append(width)
    relation, width = encoder.relation(document["root"])
    if not encoder.scans or not 1 <= width <= 1024:
        raise ValueError("Native plan must read declared tables and return 1..1024 columns")
    root = encoder.plan.relations.add().root
    root.input.CopyFrom(relation)
    root.names.extend(f"c{i}" for i in range(width))
    return encoder.plan.SerializeToString(), encoder.scans
