.PHONY: test build publish clean

test:
	python -m unittest tests

build:
	python setup.py sdist bdist_wheel

publish:
	python -m twine upload dist/*

clean:
	rm -rf dist build deta.egg.egg-info