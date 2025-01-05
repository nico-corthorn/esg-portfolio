AWS_REGION := $(shell aws configure get region)
SAGEMAKER_BUCKET := $(shell aws sts get-caller-identity --query 'Account' --output text | xargs -I {} echo "sagemaker-$(AWS_REGION)-{}")

install:
	python -m pip install --upgrade pip &&\
		pip install -r lambda_requirements.txt &&\
		pip install -r dev_requirements.txt &&\
		pip install -r tests/test_requirements.txt &&\
		pip install -r esgtools/sentiment/inference_pipeline/requirements.txt &&\
		pip install -e . --use-pep517

update-conda-env:
	conda remove --name esg-env --all -y || true
	conda create --name esg-env python=3.9 -y
	conda init bash
	# You may need to run the following command manually in terminal
	conda activate esg-env
	make install

format:
	black esgtools tests
	isort esgtools tests

lint:
	pylint esgtools tests --rcfile=.pylintrc
	black --check --diff esgtools tests
	isort --check-only esgtools tests

test:
	python -m pytest -v

pre_pr: format lint test

build:
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

deploy:
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

build-esgtools-package:
	@echo "Building esgtools package..."
	rm -rf dist
	mkdir -p dist
	python setup.py sdist
	mv dist/*.tar.gz dist/esgtools.tar.gz

upload-sentiment-code: build-esgtools-package
	aws s3 cp ./esgtools/sentiment/inference_pipeline/preprocessing.py s3://$(SAGEMAKER_BUCKET)/sentiment/code/
	aws s3 cp ./esgtools/sentiment/inference_pipeline/inference.py s3://$(SAGEMAKER_BUCKET)/sentiment/code/
	aws s3 cp ./esgtools/sentiment/inference_pipeline/serve s3://$(SAGEMAKER_BUCKET)/sentiment/code/
	aws s3 cp ./esgtools/sentiment/inference_pipeline/model_config.json s3://$(SAGEMAKER_BUCKET)/sentiment/config/
	aws s3 cp ./esgtools/sentiment/inference_pipeline/prompt_template.txt s3://$(SAGEMAKER_BUCKET)/sentiment/config/
	aws s3 cp ./dist/esgtools.tar.gz s3://$(SAGEMAKER_BUCKET)/sentiment/packages/

free-docker-memory:
	docker system prune -f
	docker image prune -a
	docker volume prune -f

push-sentiment-container:
	chmod +x ./esgtools/sentiment/inference_pipeline/build_and_push.sh
	./esgtools/sentiment/inference_pipeline/build_and_push.sh

deploy-sentiment-pipeline:
	make upload-sentiment-code
	python ./esgtools/sentiment/inference_pipeline/deploy_pipeline.py