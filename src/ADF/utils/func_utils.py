import logging

from typing import Type, Callable, Union, Dict, List, Set, Any

from ADF.components.data_structures import AbstractDataStructure


def nested_concretize(
    arg: Any, concrete_class: Type, abstraction_params: List, ads_classes: Set
) -> Any:
    if isinstance(arg, list) or isinstance(arg, tuple):
        return [
            nested_concretize(item, concrete_class, abstraction_params, ads_classes)
            for item in arg
        ]
    elif isinstance(arg, dict):
        return {
            key: nested_concretize(val, concrete_class, abstraction_params, ads_classes)
            for key, val in arg.items()
        }
    elif isinstance(arg, AbstractDataStructure):
        concretization = arg.concretize(concrete_class)
        abstraction_params.append(concretization[1])
        ads_classes.add(type(arg))
        return concretization[0]
    else:
        return arg


# Transition class will have converted all data to output layer DS so all inputs are the same DS type even when combining
# => abstract method can be taken from any DS
# => abstraction params can be taken from any concretization
# WARNING : will only concretize kwargs one layer deep (important when concretizing a custom data load)
def concretize(concrete_class: Type):
    def decorator(func: Callable):
        def wrapper(
            *args, **kwargs
        ) -> Union[AbstractDataStructure, Dict[str, AbstractDataStructure]]:
            abstraction_params = []
            ads_classes = set()
            concrete_args = nested_concretize(
                args, concrete_class, abstraction_params, ads_classes
            )
            concrete_kwargs = nested_concretize(
                kwargs, concrete_class, abstraction_params, ads_classes
            )
            if len(ads_classes) > 1:
                raise ValueError(
                    f"Multiple Abstract Data Structure classes found in concretization : {ads_classes}."
                )
            abstraction_params = abstraction_params[0] if abstraction_params else {}
            result: Union[concrete_class, Dict[str, concrete_class]] = func(
                *concrete_args, **concrete_kwargs
            )
            if len(ads_classes) is None:
                logging.warning(
                    "WARNING : pointless concretization as no ADS class was found in input"
                )
                return result
            ads_class = ads_classes.pop()
            if isinstance(result, concrete_class):
                return ads_class.abstract(result, abstraction_params)
            elif isinstance(result, dict):
                result: Dict[str, concrete_class]
                ret = {
                    key: ads_class.abstract(val, abstraction_params)
                    for key, val in result.items()
                }
                return ret
            else:
                raise ValueError(
                    f"Unhandled output value type '{result.__class__.__name__}'."
                )

        return wrapper

    return decorator
