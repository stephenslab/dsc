help:
	@grep '^[^#[:space:]].*:' Makefile | tr -d '\n' | sed 's/:/ /g'
	@echo

sos:
	cd dsc; svn checkout https://github.com/bpeng2000/SOS/trunk/pysos; cd -

install:
	python setup.py install

utest:
	cd test; python run_tests.py; cd -

clean:
	rm -rf *.egg-info build dist

purge:
	git clean -df
