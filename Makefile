spark_version := 2.4.8
hadoop_version := 2.7
spark_home := spark-${spark_version}-bin-hadoop${hadoop_version}
spark_tgz_url := https://downloads.apache.org/spark/spark-${spark_version}/${spark_home}.tgz
ima_notebook := algorithm.ipynb

venv: requirements.txt
	test -d venv || python3 -m venv venv
	. venv/bin/activate; pip3 install -Ur requirements.txt;

install_spark:
	test -d ${spark_home} || (wget ${spark_tgz_url}; tar -xzvf ${spark_home}.tgz)

clean_spark:
	rm -r ${spark_home}; rm -rf ${spark_home}.tgz

flake8:	venv
	# stop the build if there are Python syntax errors or undefined names in *.py file
	. venv/bin/activate; flake8 *.py dataset_metrics/ etl/ tests/ --count --select=E9,F63,F7,F82 --show-source --statistics
	# exit-zero treats all errors as warnings. The GitHub editor is 127 chars wide
	. venv/bin/activate; flake8 *.py dataset_metrics/ etl/ tests/ --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

test:	venv
	. venv/bin/activate; PYTHONPATH=${PYTHONPATH}:etl/ pytest --cov etl tests/

py:	venv
	# nbconvert output is saved as <basename>.py
	. venv/bin/activate; jupyter nbconvert ${ima_notebook} --to script
