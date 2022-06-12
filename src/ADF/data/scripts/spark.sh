#!/bin/bash

set -euo pipefail

TYPE=local-mono-layer
MONO_TYPE=.spark
EXE=adf-launcher.py
ICP=config/implementers/implementer.${TYPE}${MONO_TYPE}.yaml
FCP=config/flows/flows.${TYPE}.yaml

echo "Destroying prior runs..."
$EXE $ICP setup-implementer -d
echo "Setting up implementer..."
$EXE $ICP setup-implementer
echo "Setting up flows..."
$EXE $ICP setup-flows $FCP
echo "Copying files..."
cp data_samples/test.csv local_implementers/mono_layer_spark/light/mono/test-flow/landing-step/default/0.csv
cp data_samples/test.csv local_implementers/mono_layer_spark/light/mono/test-flow/landing-step/default/1.csv
cp data_samples/test.csv local_implementers/mono_layer_spark/light/mono/test-flow/landing-step/default/3.csv
cp data_samples/test.csv local_implementers/mono_layer_spark/light/mono/extra-flow/landing-step/default/0.csv
cp data_samples/test.csv local_implementers/mono_layer_spark/light/mono/extra-flow/landing-step/default/1.csv
echo "Running orchestrator..."
$EXE $ICP orchestrate $FCP -s
