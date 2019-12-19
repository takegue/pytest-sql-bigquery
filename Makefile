
.PHONY: install test

install:
	poetry install

test:
	poetry run py.test -vv ./tests

publish:
	poetry publish --build -r testpypi
