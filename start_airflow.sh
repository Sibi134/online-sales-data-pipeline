#!/bin/bash
source .venv/bin/activate
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
airflow standalone

