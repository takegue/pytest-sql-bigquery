
.PHONY: install test

install:
	poetry install

test:
	poetry run py.test -vv ./test

publish:
	poetry publish -r testpypi
