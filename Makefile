test: unittest

unittest:
	pytest --doctest-modules --last-failed --durations=3

format:
	autoflake chunksum/*.py
	isort chunksum/*.py
