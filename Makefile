.PHONY: test build publish clean

test:
	python -m unittest tests

test_email:
	python -m unittest tests.TestSendEmail

build:
	python setup.py sdist bdist_wheel

publish:
	python -m twine upload dist/*

clean:
	rm -rf dist build deta.egg.egg-info