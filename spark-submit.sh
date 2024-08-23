#!/bin/bash

DRIVER_PY=$1

zip -r pdp.zip pdp

spark-submit --py-files "pdp.zip,$DRIVER_PY" "$DRIVER"
