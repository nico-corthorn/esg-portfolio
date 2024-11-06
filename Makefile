install:
	python -m pip install --upgrade pip &&\
		pip install -r esgtools/requirements.txt &&\
		pip install -r tests/test_requirements.txt &&\
		pip install -r requirements-dev.txt

test:
	python -m pytest -v

format:
	black esgtools tests
	isort esgtools tests

lint:
	pylint esgtools tests --rcfile=.pylintrc
	black --check esgtools tests
	isort --check-only esgtools tests

deploy:
	sam build &&\
	sam deploy --config-file samconfig.toml

all: install format lint test
