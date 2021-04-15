.PHONY: test build publish clean
.DEFAULT_GOAL := help

test: # Run Unit Test
	@python3 -m unittest tests

test_email: # Test Send Email
	@python3 -m unittest tests.TestSendEmail

build: # Build distribution for SDK
	@python3 setup.py sdist bdist_wheel

publish: # Publish the package to PyPI
	@python3 -m twine upload dist/*

clean: # Remove distribution packages
	@rm -rf dist build deta.egg.egg-info

format: # Format using black
	@black .

check: # Check for files to format using black
	@black --check .

setup: # Developement setup for the SDK
	@pip install -e .[dev]

help: # Show this help
	@echo "Deta Python SDK"
	@egrep -h '\s#\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?# "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
