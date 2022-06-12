#!/bin/bash

set -euo pipefail

echo "Copying files..."
cp data_samples/test.csv local_implementers/multi_layer_sqlite/light/complete/main-flow/landing-step/default/0.csv
cp data_samples/test.csv local_implementers/multi_layer_sqlite/light/complete/main-flow/landing-step/default/1.csv
cp data_samples/test.csv local_implementers/multi_layer_sqlite/light/complete/main-flow/landing-step/default/3.csv
cp data_samples/test.csv local_implementers/multi_layer_sqlite/heavy/complete/secondary-flow/landing-step/default/0.csv
cp data_samples/test.csv local_implementers/multi_layer_sqlite/heavy/complete/secondary-flow/landing-step/default/1.csv
cp data_samples/test.csv local_implementers/multi_layer_sqlite/heavy/complete/secondary-flow/landing-step/default/3.csv
