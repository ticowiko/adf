#!/bin/bash

set -euo pipefail

TYPE=local-multi-layer
DB_TYPE=.sqlite
EXE=adf-launcher.py
ICP=config/implementers/implementer.${TYPE}${DB_TYPE}.yaml
FCP=config/flows/flows.${TYPE}.yaml

echo "Destroying prior runs..."
$EXE $ICP setup-implementer -d
echo "Setting up implementer..."
$EXE $ICP setup-implementer
echo "Setting up flows..."
$EXE $ICP setup-flows $FCP
echo "Copying files..."
cp data_samples/test.csv local_implementers/multi_layer_sqlite/light/multi/test-flow/landing-step/default/0.csv
cp data_samples/test.csv local_implementers/multi_layer_sqlite/light/multi/test-flow/landing-step/default/1.csv
cp data_samples/test.csv local_implementers/multi_layer_sqlite/light/multi/test-flow/landing-step/default/3.csv
cp data_samples/test.csv local_implementers/multi_layer_sqlite/light/multi/extra-flow/landing-step/default/0.csv
cp data_samples/test.csv local_implementers/multi_layer_sqlite/light/multi/extra-flow/landing-step/default/1.csv
echo "Running orchestrator..."
$EXE $ICP orchestrate $FCP -s
