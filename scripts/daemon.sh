#!/bin/bash

source .venv/bin/activate;
export DAGSTER_HOME=$(pwd)/.dagster;

dagster-daemon run;