.PHONY: test build publish

test:
	python -m unittest tests

build:
	python setup.py sdist bdist_wheel

publish:
	python -m twine upload dist/*