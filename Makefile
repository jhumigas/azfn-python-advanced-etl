OUTPUT_DIR= .

init-env-file:
	cp -n -p template.env .env | true


setup:
	poetry env use python3.9
	poetry install -vv

check-bandit:
	poetry run bandit -c pyproject.toml -r .

# Numpy safety check error are ignored du to sktime dependencies. Turn numpy to 1.22.0 to resolve safety check when
# this issue will be resolved https://github.com/alan-turing-institute/sktime/discussions/2037
check-safety:
	poetry export -f requirements.txt | poetry run safety check -i 44715 -i 44716 -i 44717 --stdin

format:
	poetry run black ./py_project

check-flake8:
	poetry run flake8

build-package:
	poetry build

build-bundle:
	poetry build --format wheel
	pip install ./dist/*.whl --target $(OUTPUT_DIR)/.python_packages/lib/site-packages

run-unit-tests:
	poetry run pytest tests/unit

run-unit-tests-cov:
	poetry run pytest tests/unit -s --cov-append --doctest-modules --junitxml=$(OUTPUT_DIR)/junit/unit-tests-results.xml --cov=py_project --cov-report=xml:$(OUTPUT_DIR)/coverage.xml --cov-report=html:$(OUTPUT_DIR)/htmlcov

run-integration-tests:
	poetry run pytest tests/integration

start-db:
	docker-compose -f ./docker/docker-compose.yml up --build --remove-orphans --force-recreate

stop-db:
	docker-compose -f ./docker/docker-compose.yml down --volumes

prepare-dev:
	cp ./docker/template.env ./docker/.env

start-dev:
	mkdir -p ./docker/function_apps ./docker/tmp
	cp -r ./function_apps/* ./docker/function_apps/
	poetry build --format wheel
	mv dist ./docker/function_apps/
	docker-compose -f ./docker/docker-compose.yml --profile local_env up --build --remove-orphans --force-recreate -d

check-up-azfn:
	time bash -x docker/wait-azfn.sh

stop-dev:
	rm -r ./docker/function_apps
	docker-compose -f ./docker/docker-compose.yml down --volumes