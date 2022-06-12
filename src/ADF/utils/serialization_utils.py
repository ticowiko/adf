from typing import List, Dict

from ADF.exceptions import MissingConfigParameter


class ToDictMixin:
    flat_attrs: List[str] = []
    deep_attrs: List[str] = []
    iter_attrs: List[str] = []

    def to_dict(self) -> Dict:
        ret = {"class_name": self.__class__.__name__}
        for attr in self.flat_attrs:
            override_method = f"get_dict_{attr}"
            if hasattr(self, override_method):
                ret[attr] = getattr(self, override_method)()
            else:
                ret[attr] = getattr(self, attr)
        for attr in self.deep_attrs:
            ret[attr] = getattr(self, attr).to_dict()
        for attr in self.iter_attrs:
            ret[attr] = []
            for item in getattr(self, attr):
                ret[attr].append(item.to_dict())
        return ret


def extract_parameter(data: Dict, param: str, info: str):
    if param not in data:
        raise MissingConfigParameter(data, param, info)
    return data[param]
