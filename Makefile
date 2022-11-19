install:
	pip install --upgrade pip &&\
		pip install -r esgtools/requirements.txt

test:
	python -m pytest -v

format:
	black *.py

lint:
	pylint --disable=R,C hello.py

all: install lint test