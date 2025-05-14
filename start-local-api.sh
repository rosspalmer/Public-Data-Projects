#!/bin/bash

uvicorn pdp.restapi.weather:app --reload --host localhost --port 8000
