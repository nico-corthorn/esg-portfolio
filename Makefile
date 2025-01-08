AWS_REGION := $(shell aws configure get region)
SAGEMAKER_BUCKET := $(shell aws sts get-caller-identity --query 'Account' --output text | xargs -I {} echo "sagemaker-$(AWS_REGION)-{}")
AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query 'Account' --output text)

install:
	python -m pip install --upgrade pip &&\
		pip install -r lambda_requirements.txt &&\
		pip install -r dev_requirements.txt &&\
		pip install -r tests/test_requirements.txt &&\
		pip install -e . --use-pep517

update-conda-env:
	conda remove --name esg-env --all -y || true
	conda create --name esg-env python=3.9 -y
	conda init bash
	# You may need to run the following command manually in terminal
	conda activate esg-env
	make install

format:
	black tba_invest_etl tests
	isort tba_invest_etl tests

lint:
	pylint tba_invest_etl tests --rcfile=.pylintrc
	black --check --diff tba_invest_etl tests
	isort --check-only tba_invest_etl tests

test:
	python -m pytest -v

pre_pr: format lint test

build-package:
	@echo "Building tba_invest_etl package..."
	rm -rf dist
	mkdir -p dist
	python setup.py sdist
	mv dist/*.tar.gz dist/tba_invest_etl.tar.gz

publish-package:
	@echo "Publishing package to AWS CodeArtifact..."
	python -m pip install --upgrade twine
	aws codeartifact login --tool twine --domain tba-investments --domain-owner $(AWS_ACCOUNT_ID) --repository tba-investments-etl
	python -m twine upload --repository codeartifact dist/*

build-sam:
	@echo "Cleaning previous build..."
	rm -rf .aws-sam/build

	@echo "Building SAM application..."
	sam build --debug

	@echo "Creating Lambda layer..."
	mkdir -p .aws-sam/build/PythonDependenciesLayer/python
	docker run --rm --platform linux/amd64 --entrypoint /bin/bash \
		-v $(PWD):/var/task \
		public.ecr.aws/lambda/python:3.9 \
		-c "pip install -r lambda_requirements.txt -t /var/task/.aws-sam/build/PythonDependenciesLayer/python"

	@echo "Installing package..."
	pip install -e . --use-pep517

	@echo "Checking build size..."
	du -sh .aws-sam/build/* 2>/dev/null | sort -hr
	@echo "Size of entire .aws-sam directory:"
	du -sh .aws-sam

deploy-local:
	@echo "Deploying from local samconfig file..."
	sam deploy --config-file samconfig.toml

deploy-sam:
	@echo "Deploying..."
	sam deploy \
		--stack-name sam-app \
		--s3-bucket aws-sam-cli-managed-default-samclisourcebucket-rluxcxhrbn0v \
		--s3-prefix sam-app \
		--region us-east-2 \
		--capabilities CAPABILITY_IAM \
		--no-confirm-changeset \
		--no-fail-on-empty-changeset \
		--disable-rollback false
