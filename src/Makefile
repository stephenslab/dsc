help:
	@grep '^[^#[:space:]].*:' Makefile | tr -d '\n' | sed 's/:/ /g'
	@echo

sos:
	svn checkout https://github.com/bpeng2000/SOS/trunk/pysos libsos

install:
	python setup.py install

utest:
	cd test; python run_tests.py; cd -

clean:
	rm -rf *.egg-info build dist

purge:
	git clean -df
