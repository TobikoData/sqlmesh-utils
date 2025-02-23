install:
	pip install -e ".[dev]"

test:
	pytest -n auto -m "not integration" --junitxml=test-results/junit-unit.xml

clone-upstream:
	if [ -d "./_sqlmesh_upstream" ]; then cd ./_sqlmesh_upstream && git pull; else git clone --depth=1 git@github.com:TobikoData/sqlmesh.git ./_sqlmesh_upstream; fi

engine-%-up:
	cd _sqlmesh_upstream && make engine-docker-${*}-up

integration-test: clone-upstream engine-trino-up engine-postgres-up
	pytest -n auto -m "integration" --junitxml=test-results/junit-integration.xml

install-pre-commit:
	pre-commit install

style:
	pre-commit run --all-files

clean:
	rm -fr *.egg-info test-results .cache _sqlmesh_upstream logs .mypy_cache .pytest_cache .ruff_cache dist

package:
	pip3 install build && python3 -m build

publish: package
	pip3 install twine && python3 -m twine upload dist/*