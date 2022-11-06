test: unittest

unittest:
	pytest --doctest-modules --last-failed --durations=3

format:
	autoflake -i chunksum/*.py
	isort chunksum/*.py
