#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cat ${SCRIPT_DIR}/stations.txt | xargs ${SCRIPT_DIR}/venv/bin/python3 main.py ${SCRIPT_DIR}/forecasts.db
