#!/bin/bash

set -e

black --line-length 120 src tests
ruff src tests --fix
ruff format src tests
