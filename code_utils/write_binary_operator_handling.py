def op_func(op):
    if op.startswith("r"):
        op = op[1:]
    return op + "_" if op in ["and", "or"] else op


def write_func(op, other):
    return f"""    def {op}_{other}(self, other{": 'AbstractDataColumn'" if other == 'col' else ': Any'}):
        return self.default_binary_operator_{other}_{"right" if op.startswith("r") else "left"}(operator.{op_func(op)}, other)
"""


def write_op(op):
    return f"""    def __{op}__(self, other):
        return self._handle_binary_operator(operator.{op_func(op)}, other, left={"False" if op.startswith("r") else "True"})
"""


def write_condition(op, other):
    return f"""            if op is operator.{op_func(op)} and {"not left" if op.startswith("r") else "left"}:
                return self.{op}_{other}(other)"""


if __name__ == "__main__":
    others = ["col", "val"]
    ops = [
        "add",
        "radd",
        "sub",
        "rsub",
        "mul",
        "rmul",
        "floordiv",
        "rfloordiv",
        "truediv",
        "rtruediv",
        "mod",
        "rmod",
        "pow",
        "rpow",
        "eq",
        "ne",
        "lt",
        "le",
        "gt",
        "ge",
        "and",
        "rand",
        "or",
        "ror",
    ]
    print(
        """    def default_binary_operator_col_left(
        self, op: Callable, other: "AbstractDataColumn"
    ) -> "AbstractDataColumn":
        raise NotImplementedError

    def default_binary_operator_col_right(
        self, op: Callable, other: "AbstractDataColumn"
    ) -> "AbstractDataColumn":
        raise NotImplementedError

    def default_binary_operator_val_left(self, op: Callable, other) -> "AbstractDataColumn":
        raise NotImplementedError

    def default_binary_operator_val_right(self, op: Callable, other) -> "AbstractDataColumn":
        raise NotImplementedError
"""
    )
    print(
        """    def _handle_binary_operator(self, op: Callable, other: Union["AbstractDataColumn", Any], left: bool):
        if isinstance(other, AbstractDataColumn):"""
    )
    for op in ops:
        print(write_condition(op, "col"))
    print(
        """            raise ValueError(f"Unknown binary operator {str(op)}.")
        else:"""
    )
    for op in ops:
        print(write_condition(op, "val"))
    print(
        """            raise ValueError(f"Unknown binary operator {str(op)}.")
"""
    )
    for other in others:
        for op in ops:
            print(write_func(op, other))
    for op in ops:
        print(write_op(op))
