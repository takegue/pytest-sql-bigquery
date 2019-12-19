
.PHONY: install test

install:
	poetry install

test:
	poetry run py.test -vv ./tests

publish:
	poetry publish -r testpypi
