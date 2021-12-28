from dataclasses import dataclass

from drama.core.model import _BaseSimpleTabularDataset
from drama.datatype import (
    DataType,
    is_integer,
    is_string,
    is_float,
    is_boolean,
)


@dataclass
class TabularDataset(_BaseSimpleTabularDataset):
    has_header: bool = is_integer()
    line_delimiter: str = is_string()
    number_of_columns: int = is_integer()
    number_of_lines: int = is_integer()


@dataclass
class MlModel(DataType):
    resource: str = is_string()
    label: str = is_string()


@dataclass
class MlModelDTC(MlModel):
    class_weight: str = is_string()
    criterion: str = is_string()
    max_depth: str = is_string()
    max_features: str = is_string()
    max_leaf_nodes: str = is_string()
    min_impurity_decrease: float = is_float()
    min_impurity_split: str = is_string()
    min_samples_leaf: int = is_integer()
    min_samples_split: int = is_integer()
    min_weight_fraction_leaf: float = is_float()
    splitter: str = is_string()


@dataclass
class MlModelSVC(MlModel):
    C: float = is_float()
    cache_size: int = is_integer()
    class_weight: str = is_string()
    coef0: float = is_float()
    decision_function_shape: str = is_string()
    degree: int = is_integer()
    gamma: str = is_string()
    kernel: str = is_string()
    max_iter: int = is_integer()
    probability: bool = is_boolean()
    shrinking: bool = is_boolean()
    tol: float = is_float()


@dataclass
class MlModelKNN(MlModel):
    n_neighbors: int = is_integer()
    algorithm: str = is_string()
    leaf_size: int = is_integer()
    metric: str = is_string()
    metric_params: str = is_string()
    p: int = is_integer()
    n_jobs: str = is_string()
    weights: str = is_string()


@dataclass
class MlModelRF(MlModel):
    bootstrap: bool = is_boolean()
    class_weight: str = is_string()
    criterion: str = is_string()
    max_depth: str = is_string()
    max_features: str = is_string()
    max_leaf_nodes: str = is_string()
    min_impurity_decrease: float = is_float()
    min_impurity_split: str = is_string()
    min_samples_leaf: int = is_integer()
    min_samples_split: int = is_integer()
    min_weight_fraction_leaf: float = is_float()
    n_estimators: int = is_integer()
    n_jobs: str = is_string()
    oob_score: bool = is_boolean()
    random_state: str = is_string()
    verbose: int = is_integer()
    warm_start: bool = is_boolean()


@dataclass
class MlModelLSTM(MlModel):
    input_size: int = is_integer()
    output_size: int = is_integer()
    n_neurons: int = is_integer()
    epochs: int = is_integer()
    batch_size: int = is_integer()
    verbose: int = is_integer()
