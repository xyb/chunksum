pre-commit:
	pre-commit run --all-files

test: unittest

unittest:
	pytest --doctest-modules --last-failed --durations=3

coverage:
	pytest --doctest-modules --durations=3 --cov --cov-report term-missing --cov-report html --cov-report xml
