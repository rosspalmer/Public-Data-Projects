#!/bin/bash

DRIVER_PY=$1

zip -r pdp.zip pdp

pip install -t dependencies -r requirements.txt
pushd dependencies
  zip -r ../dependencies.zip .
popd

spark-submit --py-files "pdp.zip,dependencies.zip,$DRIVER_PY" "$DRIVER_PY"
