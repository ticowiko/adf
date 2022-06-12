from typing import List, Optional, Any, Dict

import ADF


class ADFException(Exception):
    pass


class MetaCheckFailure(ADFException):
    def __init__(self, reason):
        super().__init__(f"Failed meta check : '{reason}'.")


class UnsupportedType(ADFException):
    def __init__(self, t, where):
        super().__init__(f"Unsupported type(s) {str(t)} in {str(where)}.")


class CombinationStepError(ADFException):
    def __init__(
        self,
        step: "ADF.components.flow_config.ADFStep",
        context: str,
        msg: str,
    ):
        super().__init__(f"COMBINATION STEP {str(step)}::{str(context)} FAILED : {msg}")


class InvalidInputStep(ADFException):
    def __init__(
        self,
        input_step: "ADF.components.flow_config.ADFStep",
        combination_step: "ADF.components.flow_config.ADFCombinationStep",
    ):
        super().__init__(
            f"Step {str(input_step)} is not a valid input for combination step {str(combination_step)}."
        )


class ADLUnsupportedTransition(ADFException):
    def __init__(
        self,
        transition: "ADF.components.layer_transitions.ADLTransition",
        reason: Optional[str] = None,
    ):
        super().__init__(
            f"'{str(transition)}' transition not supported{f' : {reason}' if reason else ''}."
        )


class ADLUnhandledTransition(ADFException):
    def __init__(self, implementer, step_in, step_out):
        super().__init__(
            f"'{implementer.__class__.__name__}' implementer has no transition from layer '{step_in.layer}' to '{step_out.layer}' (for {str(step_in)} -> {str(step_out)})."
        )


class ColumnNotFound(ADFException):
    def __init__(
        self, col_name: str, ads: "ADF.components.data_structures.AbstractDataStructure"
    ):
        super().__init__(
            f'No column "{col_name}" found, candidates are "{", ".join(ads.list_columns())}."'
        )


class JoinError(ADFException):
    def __init__(self, msg):
        super().__init__(f"JOIN ERROR : {msg}")


class GroupByError(ADFException):
    def __init__(self, msg):
        super().__init__(f"GROUP BY ERROR : {msg}")


class UnionError(ADFException):
    def __init__(self, msg):
        super().__init__(f"UNION ERROR : {msg}")


class DistinctError(ADFException):
    def __init__(self, msg):
        super().__init__(f"DISTINCT ERROR : {msg}")


class InputError(ADFException):
    pass


class UnknownBatch(ADFException):
    def __init__(self, step: "ADF.components.flow_config.ADFStep", batch_id: str):
        super().__init__(f"No batch '{batch_id}' found in step '{str(step)}'.")


class CorruptedBatch(ADFException):
    def __init__(self, step: "ADF.components.flow_config.ADFStep", batch_id: str):
        super().__init__(
            f"Multiple statuses for '{batch_id}' found in step '{str(step)}'."
        )


class UnknownStep(ADFException):
    def __init__(
        self, flow: "ADF.components.flow_config.AbstractDataFlow", step_name: str
    ):
        super().__init__(f"No step {step_name} found in flow {flow.name}.")


class NoPreviousStep(ADFException):
    def __init__(self, step: "ADF.components.flow_config.ADFStep"):
        super().__init__(f"Could not find previous step of {str(step)}")


class NoNextStep(ADFException):
    def __init__(self, step: "ADF.components.flow_config.ADFStep"):
        super().__init__(f"Could not find next step of {str(step)}")


class UnknownFlow(ADFException):
    def __init__(self, flow_name: str):
        super().__init__(f"No flow {flow_name} found.")


class InvalidConfig(ADFException):
    def __init__(self, errs: List[str]):
        self.errs = errs
        sep = "\n - "
        super().__init__(f"Invalid config :{sep}{sep.join(errs)}")


class InvalidAbstractDataFlow(InvalidConfig):
    pass


class InvalidADFStep(InvalidConfig):
    pass


class InvalidMeta(InvalidConfig):
    pass


class InvalidMetaColumn(InvalidConfig):
    pass


class InvalidADFCollection(InvalidConfig):
    pass


class MissingConfigParameter(ADFException):
    def __init__(self, data: Dict, param: str, info: str):
        super().__init__(
            f"Parameter '{param}' not found for '{info}', provided parameters are : '{', '.join(data.keys())}'."
        )


class ImproperOperation(ADFException):
    def __init__(self, msg: str):
        super().__init__(msg)


class MismatchedSQLSubqueries(ADFException):
    def __init__(self, obj1, obj2, operation):
        super().__init__(
            f"Cannot apply '{operation}' between objects of type '{obj1.__class__.__name__}' and '{obj2.__class__.__name__}' with mismatched subqueries."
        )


class UnhandledGroupOperation(ADFException):
    def __init__(self, obj, method):
        super().__init__(
            f"Cannot apply '{method}' while on grouping object of type '{obj.__class__.__name__}'."
        )


class UnhandledMeta(ADFException):
    def __init__(
        self,
        layer: "ADF.components.layers.AbstractDataLayer",
        step: "ADF.components.flow_config.ADFStep",
        extra: List[str],
        missing: List[str],
    ):
        super().__init__(
            f"'{str(layer)}' cannot handle meta of step {str(step)}. Required meta is {{extra: {extra}, missing: {missing}}}."
        )


class UnhandledConcretization(ADFException):
    def __init__(self, concrete_class):
        super().__init__(f"Unsupported concretization of {concrete_class.__name__}.")


class AWSADFException(ADFException):
    pass


class AWSFailedConfigFetch(AWSADFException):
    def __init__(self, connector: "ADF.utils.AWSResourceConnector"):
        super().__init__(
            f"Failed to fetch config for '{connector}', are you sure the resource was created ?"
        )


class AWSUnmanagedOperation(AWSADFException):
    def __init__(self, layer: Any, operation: str):
        super().__init__(
            f"Object '{str(layer)}' does not support operation '{operation}'. Try running with the corresponding managed object."
        )
