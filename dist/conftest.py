import pytest

from sqlchecker.integrations.pytest import SQLReaderForChecking

class ChainPytestFile(pytest.File):

    def __init__(self, path, parent, chains, **kwargs):
        super().__init__(path, parent, **kwargs)
        self.chains = chains

    def collect(self):
        for interpreter in self.chains:
            yield from interpreter.collect()

def pytest_collect_file(parent, path):
    if path.ext == ".sql":
        return ChainPytestFile(
            path, parent,
            [
                SQLReaderForChecking(path, parent),
            ])
