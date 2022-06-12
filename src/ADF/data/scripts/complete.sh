#!/bin/bash

set -euo pipefail

EXE=adf-launcher.py
ICP=config/implementers/implementer.local-multi-layer.sqlite.yaml
FCP=config/flows/flows.complete.yaml

echo "Destroying prior runs..."
$EXE $ICP setup-implementer -d
echo "Setting up implementer..."
$EXE $ICP setup-implementer
echo "Setting up flows..."
$EXE $ICP setup-flows $FCP
echo "Copying files..."
cp data_samples/test.csv local_implementers/multi_layer_sqlite/light/complete/main-flow/landing-step/default/0.csv
cp data_samples/test.csv local_implementers/multi_layer_sqlite/light/complete/main-flow/landing-step/default/1.csv
cp data_samples/test.csv local_implementers/multi_layer_sqlite/light/complete/main-flow/landing-step/default/3.csv
cp data_samples/test.csv local_implementers/multi_layer_sqlite/heavy/complete/secondary-flow/landing-step/default/0.csv
cp data_samples/test.csv local_implementers/multi_layer_sqlite/heavy/complete/secondary-flow/landing-step/default/1.csv
cp data_samples/test.csv local_implementers/multi_layer_sqlite/heavy/complete/secondary-flow/landing-step/default/3.csv
echo "Running orchestrator..."
$EXE $ICP orchestrate $FCP -s
echo "Resetting state..."
$EXE $ICP reset-batches $FCP main-flow meta-step
$EXE $ICP reset-batches $FCP secondary-flow meta-step
echo "Running orchestrator again..."
$EXE $ICP orchestrate $FCP -s
