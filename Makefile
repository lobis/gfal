NAME = gfal-cli
NAME_DIST = gfal_cli
SPECFILE = $(NAME).spec
DIST_DIR = dist
RPMBUILD = $(shell pwd)/rpmbuild

.PHONY: all clean dist srpm rpm prepare

all: dist

clean:
	rm -rf $(DIST_DIR)
	rm -rf $(RPMBUILD)
	rm -f src/gfal_cli/_version.py

dist: clean
	python3 -m pip install --upgrade build hatchling hatch-vcs
	python3 -m build

prepare: dist
	@VERSION=$$(python3 -m hatchling version | sed 's/\+.*//'); \
	mkdir -p $(RPMBUILD)/BUILD $(RPMBUILD)/RPMS $(RPMBUILD)/SOURCES $(RPMBUILD)/SPECS $(RPMBUILD)/SRPMS; \
	cp $(DIST_DIR)/$(NAME_DIST)-$${VERSION}-py3-none-any.whl $(RPMBUILD)/SOURCES/; \
	cp $(SPECFILE) $(RPMBUILD)/SPECS/

srpm: prepare
	@FULL_VERSION=$$(python3 -m hatchling version); \
	VERSION=$$(echo $${FULL_VERSION} | sed 's/\+.*//'); \
	RELEASE=$$(echo $${FULL_VERSION} | grep -o '+.*' | sed 's/+/./'); \
	rpmbuild -bs $(RPMBUILD)/SPECS/$(SPECFILE) \
		--nodeps \
		--define "_topdir $(RPMBUILD)" \
		--define "version $${VERSION}" \
		--define "release $${RELEASE:-1}"

rpm: srpm
	@FULL_VERSION=$$(python3 -m hatchling version); \
	VERSION=$$(echo $${FULL_VERSION} | sed 's/\+.*//'); \
	RELEASE=$$(echo $${FULL_VERSION} | grep -o '+.*' | sed 's/+/./'); \
	rpmbuild -bb $(RPMBUILD)/SPECS/$(SPECFILE) \
		--nodeps \
		--define "_topdir $(RPMBUILD)" \
		--define "version $${VERSION}" \
		--define "release $${RELEASE:-1}"
