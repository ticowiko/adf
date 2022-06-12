import json

from sqlalchemy import String, Float, Integer

from ADF.components.state_handlers import BatchStatus


def recursive_class_naming(classes):
    return {key.__name__: recursive_class_naming(val) for key, val in classes.items()}


def recursive_bases(classes):
    for cls in classes:
        if cls is not type:
            classes[cls] = {base: [] for base in cls.__bases__}
            recursive_bases(classes[cls])


def get_recursive_bases(cls):
    classes = {cls: []}
    recursive_bases(classes)
    return json.dumps(recursive_class_naming(classes), indent=2)


if __name__ == "__main__":
    print(get_recursive_bases(type(BatchStatus.batch_id)))
    print(get_recursive_bases(type(BatchStatus.batch_id + BatchStatus.batch_id)))
    print(get_recursive_bases(type(~BatchStatus.batch_id)))
    print(get_recursive_bases(String))
    print(get_recursive_bases(Float))
    print(get_recursive_bases(Integer))
