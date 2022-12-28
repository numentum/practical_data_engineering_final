#!/bin/bash

source .venv/bin/activate;
export DAGSTER_HOME=$(pwd)/.dagster;

source .env;

dagster-daemon run;