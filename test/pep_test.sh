#/bin/sh

flake8 --ignore=W503,E123 *.py src/sender/*.py src/sender/eventDetectors/*.py src/sender/dataFetchers/*.py src/shared/*.py src/hidra_control/*.py src/APIs/*.py src/receiver/*.py test/API/*.py
