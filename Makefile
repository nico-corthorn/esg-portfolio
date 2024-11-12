install:
	python -m pip install --upgrade pip &&\
		pip install -r dev_requirements.txt &&\
		pip install -r lambda_requirements.txt &&\
		pip install -r tests/test_requirements.txt &&\
		pip install -e . --use-pep517

format:
	black esgtools tests
	isort esgtools tests

lint:
	pylint esgtools tests --rcfile=.pylintrc
	black --check esgtools tests
	isort --check-only esgtools tests

test:
	python -m pytest -v

pre_pr: format lint test

sambuild:
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

samdeploy-local:
	@echo "Deploying from local samconfig file..."
	sam deploy --config-file samconfig.toml

samdeploy:
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
