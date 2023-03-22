.PHONY: test build publish clean
.DEFAULT_GOAL := help

test: # Run Unit Test
	poetry run pytest tests/

test_email: # Test Send Email
	poetry run pytest tests/ -k "TestSendEmail"

build: # Build distribution for SDK
	poetry build

publish: # Publish the package to PyPI
	poetry publish --build

clean: # Remove distribution packages
	rm -rf dist/

lint: # Check and format codes with pre-commit
	poetry run pre-commit run -a

help: # Show this help
	@echo "Deta Python SDK"
	@egrep -h '\s#\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?# "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
