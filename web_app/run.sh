#!/bin/bash
CUR_DIR=$(dirname ${BASH_SOURCE})

export FLASK_APP=${CUR_DIR}/run.py
flask run --host=0.0.0.0