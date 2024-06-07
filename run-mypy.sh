#!/bin/bash

set -ex

mypy --strict skvlite/ test/
