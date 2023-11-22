#!/bin/bash

cd "$(dirname "$0")"
pip install opendatasets
pip install pandas

python3 ZainCode.py
