help:
	@grep '^[^#[:space:]].*:' Makefile | tr -d '\n' | sed 's/:/ /g'
	@echo

pysos:
	svn checkout https://github.com/gaow/SOS/trunk/pysos

install:
	python setup.py install

test:
	cd test; python run_tests.py; cd -

clean:
	rm -rf *.egg-info build dist

purge:
	git clean -df
