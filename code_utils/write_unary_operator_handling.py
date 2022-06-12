def write_condition(op):
    return f"""        if op is operator.{op}:
            return self.{op}_unary()"""


def write_func(op):
    return f"""    def {op}_unary(self):
        return self.default_unary_operator(operator.{op})
"""


def write_op(op):
    return f"""    def __{op}__(self):
        return self._handle_unary_operator(operator.{op})
"""


if __name__ == "__main__":
    ops = [
        "neg",
        "pos",
        "abs",
        "invert",
    ]
    print(
        """    def default_unary_operator(self, op: Callable) -> "AbstractDataColumn":
        raise NotImplementedError
"""
    )
    print("""    def _handle_unary_operator(self, op: Callable):""")
    for op in ops:
        print(write_condition(op))
    print(
        """        raise ValueError(f"Unknown unary operator {str(op)}.")
"""
    )
    for op in ops:
        print(write_func(op))
    for op in ops:
        print(write_op(op))
