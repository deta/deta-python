.PHONY: test build publish clean
.DEFAULT_GOAL := help

test: # Run Unit Test
	pytest tests

test_email: # Test Send Email
	pytest tests -k "TestSendEmail"

build: # Build distribution for SDK
	python setup.py sdist bdist_wheel

publish: # Publish the package to PyPI
	python -m twine upload dist/*

clean: # Remove distribution packages
	rm -rf dist build deta.egg.egg-info

format: # Format using black
	@black -l 100 .

check: # Check for files to format using black
	@black -l 100 --check --diff .

help: # Show this help
	@echo "Deta Python SDK"
	@egrep -h '\s#\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?# "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'