venv: requirements.txt
	test -d venv || virtualenv --python=$(shell which python3) venv
	. venv/bin/activate; pip install -Ur requirements.txt;

test:	venv
	. venv/bin/activate; pytest --cov etl
