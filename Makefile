NAME = gfal-cli
NAME_DIST = gfal_cli
SPECFILE = $(NAME).spec
VERSION = $(shell .venv/bin/python3 -c 'from src.gfal_cli._version import __version__; print(__version__)' | sed 's/\+.*//')
RELEASE = $(shell .venv/bin/python3 -c 'from src.gfal_cli._version import __version__; print(__version__)' | grep -o '+.*' | sed 's/+/./' || echo "1")
DIST_DIR = dist
RPMBUILD = $(shell pwd)/rpmbuild

.PHONY: all clean dist srpm rpm

all: dist

clean:
	rm -rf $(DIST_DIR)
	rm -rf $(RPMBUILD)
	rm -f src/gfal_cli/_version.py

dist: clean
	python3 -m pip install --upgrade build hatchling hatch-vcs
	python3 -m build

prepare: dist
	mkdir -p $(RPMBUILD)/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
	cp $(DIST_DIR)/$(NAME_DIST)-$(VERSION)*.tar.gz $(RPMBUILD)/SOURCES/$(NAME)-$(VERSION).tar.gz
	cp $(SPECFILE) $(RPMBUILD)/SPECS/

srpm: prepare
	rpmbuild -bs $(SPECFILE) \
		--define "_topdir $(RPMBUILD)" \
		--define "version $(VERSION)" \
		--define "release $(RELEASE)"

rpm: srpm
	rpmbuild -bb $(SPECFILE) \
		--define "_topdir $(RPMBUILD)" \
		--define "version $(VERSION)" \
		--define "release $(RELEASE)"
