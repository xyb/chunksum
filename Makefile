test: unittest

unittest:
	pytest --doctest-modules --last-failed --durations=3

style:
	autoflake
