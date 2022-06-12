#!/bin/bash

set -euo pipefail

EXE=adf-launcher.py
ICP=config/implementers/implementer.aws.yaml
PBICP=config/implementers/implementer.aws.pb.yaml
FCP=config/flows/flows.complete.yaml

#echo "Destroying prior runs..."
#$EXE $ICP setup-implementer -d
#rm -f $PBICP
echo "Setting up implementer..."
$EXE $ICP setup-implementer
echo "Outputting prebuilt implementer..."
$EXE $ICP output-prebuilt $PBICP
echo "Setting up flows..."
$EXE $PBICP setup-flows $FCP
echo "Copying files..."
aws s3 cp data_samples/test.csv s3://adf-bucket-3d05fc66-0f12-11ec-8eee-9cb6d0dc2783/ADF_S3_PREFIX/data_layers/light/complete/main-flow/landing-step/default/0.csv
aws s3 cp data_samples/test.csv s3://adf-bucket-3d05fc66-0f12-11ec-8eee-9cb6d0dc2783/ADF_S3_PREFIX/data_layers/light/complete/main-flow/landing-step/default/1.csv
aws s3 cp data_samples/test.csv s3://adf-bucket-3d05fc66-0f12-11ec-8eee-9cb6d0dc2783/ADF_S3_PREFIX/data_layers/light/complete/main-flow/landing-step/default/3.csv
aws s3 cp data_samples/test.csv s3://adf-bucket-3d05fc66-0f12-11ec-8eee-9cb6d0dc2783/ADF_S3_PREFIX/data_layers/heavy/complete/secondary-flow/landing-step/default/0.csv
aws s3 cp data_samples/test.csv s3://adf-bucket-3d05fc66-0f12-11ec-8eee-9cb6d0dc2783/ADF_S3_PREFIX/data_layers/heavy/complete/secondary-flow/landing-step/default/1.csv
aws s3 cp data_samples/test.csv s3://adf-bucket-3d05fc66-0f12-11ec-8eee-9cb6d0dc2783/ADF_S3_PREFIX/data_layers/heavy/complete/secondary-flow/landing-step/default/3.csv
echo "Running orchestrator..."
$EXE $PBICP orchestrate $FCP
