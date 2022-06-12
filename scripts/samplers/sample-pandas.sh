#!/bin/bash

set -euo pipefail

echo "Copying files..."
cp data_samples/test.csv local_implementers/mono_layer_pandas/light/mono/main-flow/landing-step/default/0.csv
cp data_samples/test.csv local_implementers/mono_layer_pandas/light/mono/main-flow/landing-step/default/1.csv
cp data_samples/test.csv local_implementers/mono_layer_pandas/light/mono/main-flow/landing-step/default/3.csv
cp data_samples/test.csv local_implementers/mono_layer_pandas/heavy/mono/secondary-flow/landing-step/default/0.csv
cp data_samples/test.csv local_implementers/mono_layer_pandas/heavy/mono/secondary-flow/landing-step/default/1.csv
cp data_samples/test.csv local_implementers/mono_layer_pandas/heavy/mono/secondary-flow/landing-step/default/3.csv
