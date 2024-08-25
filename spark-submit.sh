#!/bin/bash

DRIVER_PY=$1

zip -r pdp.zip pdp

pip install -t dependencies -r requirements.txt
popd dependencies
  zip -r ../dependencies.zip
pushd

spark-submit --py-files "pdp.zip,dependencies.zip,$DRIVER_PY" "$DRIVER"
