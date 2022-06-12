from importlib import import_module
from typing import Callable, Dict, Any, List
from typing_extensions import Literal

from ADF.utils import extract_parameter


class ADFModule:
    modules: Dict[str, "ADFModule"] = {}

    def __init__(self, name: str, import_path: str):
        self.import_path = import_path
        ADFModule.modules[name] = self

    @classmethod
    def from_config(cls, config: List):
        for module_config in config:
            cls(**module_config)


class ADFFunction:
    def __init__(self, load_as: Literal["eval", "module"], params: Dict):
        self.load_as = load_as
        self.params = params

    def __call__(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    @property
    def func(self) -> Callable:
        if self.load_as == "eval":
            return eval(
                extract_parameter(self.params, "expr", "ADFFunction 'eval' loader")
            )
        elif self.load_as == "module":
            return getattr(
                import_module(
                    ADFModule.modules[
                        extract_parameter(
                            self.params, "module", "ADFFunction 'module' loader"
                        )
                    ].import_path
                ),
                extract_parameter(self.params, "name", "ADFFunction 'module' loader"),
            )
        else:
            raise ValueError(
                f"Unknown {self.__class__.__name__} func loading method '{self.load_as}'."
            )

    @classmethod
    def from_config(cls, config: Dict) -> "ADFFunction":
        if extract_parameter(config, "load_as", "ADFFunction loader") not in [
            "eval",
            "module",
        ]:
            raise ValueError(
                f"Unknown {cls.__name__} func loading method '{config['load_as']}'."
            )
        return cls(
            load_as=extract_parameter(config, "load_as", "ADFFunction loader"),
            params=extract_parameter(config, "params", "ADFFunction loader"),
        )
