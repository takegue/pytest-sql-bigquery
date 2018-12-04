
.PHONY: install test

install:
	pipenv sync

test:
	pipenv run py.test -vv ./test
