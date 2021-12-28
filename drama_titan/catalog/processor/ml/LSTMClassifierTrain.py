from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
from drama.process import Process
from keras.layers import Bidirectional
from keras.layers import Dense
from keras.layers import Dropout
from keras.layers import GlobalMaxPool1D
from keras.layers import Input
from keras.layers import LSTM
from keras.models import Model
from sklearn.model_selection import StratifiedShuffleSplit

from drama_titan.model import MlModelLSTM, TabularDataset


def read_data_with_sliding_window(
    time_window_: int, stride_: int, file: str, delimiter: str, classification_label: str, ignore_columns=None
):

    # Read dataset
    train_df = pd.read_csv(file, delimiter=delimiter)

    if ignore_columns is not None:
        train_df.drop(ignore_columns, axis=1, inplace=True)

    x, y = [], []

    # remove label column
    features = list(train_df.columns.values)
    features.remove(classification_label)
    data = train_df[features]

    label_values = train_df[classification_label].unique()
    class_index = -1

    for class_ in label_values:
        class_index += 1
        offset = 0
        data_subset = data[train_df[classification_label] == class_].values
        rows = data_subset.shape[0]

        while (offset + time_window_) < rows:
            sample = data_subset[offset : (offset + time_window_)]
            x.append(sample)
            class_value = np.zeros(len(label_values))
            class_value[class_index] = 1.0
            y.append(class_value)
            offset += stride_

    x = np.array(x)
    y = np.array(y)

    return x, y


def create_bidirectional_lstm(input_size: tuple, output_size: int, neurons: int = 50):
    # Input layer
    inputs1 = Input(shape=input_size)

    # Hidden layers
    bidirect = Bidirectional(LSTM(neurons, return_sequences=True, dropout=0.1, recurrent_dropout=0.1))(inputs1)
    maxpool = GlobalMaxPool1D()(bidirect)
    dense1 = Dense(neurons, activation="relu")(maxpool)
    drop = Dropout(0.1)(dense1)

    # Output layer
    out = Dense(output_size, activation="sigmoid")(drop)

    # Create model
    model_ = Model(inputs=inputs1, outputs=out)
    model_.compile(loss="categorical_crossentropy", optimizer="adam", metrics=["accuracy"])
    return model_


def execute(
    pcs: Process,
    label: str,
    time_window: int = 1000,
    stride: int = 10,
    n_neurons: int = 50,
    ignore_columns=None,
    epochs: int = 3,
    batch_size: int = 16,
    verbose: int = 2,
):
    """
    Long Short-Term Memory Recurrent Neural Network training stage.

    Args:
        pcs (Process)

    Parameters:
        label (str): Classification column name
        time_window (int): Size of the sliding window. Defaults to 1000.
        stride (int): Movement of the sliding window. Defaults to 10.
        n_neurons (int): Number of neurons for the hidden layers of the LSTM. Defaults to 50.
        ignore_columns (list): List of column names to ignore during training. Defaults to [].
        epochs (int): Number of epochs to train. Defaults to 3.
        batch_size (int): Batch size for training. Defaults to 16.
        verbose (int): Verbosity of the training. 0 - silent, 1 - progress bar, 2 - number of epoch. Defaults to 2.

    Inputs:
        TabularDataset: Input dataset

    Outputs:
        MlModelLSTM: Machine learning LSTM model

    Produces:
        lstm_model.h5 (Path)

    Author:
        Khaos Research
    """
    if ignore_columns is None:
        ignore_columns = []

    # read inputs
    inputs = pcs.get_from_upstream()

    input_file: Dict[TabularDataset] = inputs["TabularDataset"][0]
    input_file_resource = input_file["resource"]
    input_file_delimiter = input_file["delimiter"]

    local_input_file = pcs.storage.get_file(input_file_resource)

    # read tabular dataset and create x and y dataframe
    pcs.info(f"Reading dataset using a sliding window of size {time_window}")

    x, y = read_data_with_sliding_window(
        time_window, stride, local_input_file, input_file_delimiter, label, ignore_columns
    )

    # train model
    lstm = create_bidirectional_lstm(x.shape[1:], y.shape[1], n_neurons)

    pcs.info(f"Training the model with a batch size of{batch_size} and {epochs} epochs")

    skf = StratifiedShuffleSplit(n_splits=10, random_state=0)
    for train_index, test_index in skf.split(x, y):
        xTest = x[test_index]
        xTrain = x[train_index]
        yTest = y[test_index]
        yTrain = y[train_index]
        lstm.fit(xTrain, yTrain, validation_data=(xTest, yTest), epochs=epochs, batch_size=batch_size, verbose=verbose)
        # This break is added in cases where OoM errors occur if input
        # files are big.
        break

    # send to remote storage
    pcs.info([f"Saving to disk"])
    out_model = Path(pcs.storage.local_dir, "lstm_model.h5")

    lstm.save(out_model)

    dfs_dir = pcs.storage.put_file(out_model)

    # send to downstream
    model = MlModelLSTM(
        resource=dfs_dir,
        label=label,
        input_size=time_window,
        output_size=y.shape[1],
        n_neurons=n_neurons,
        epochs=epochs,
        batch_size=batch_size,
        verbose=verbose,
    )

    pcs.to_downstream(model)

    return {"output": model, "resource": dfs_dir}
