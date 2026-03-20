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
	cp $(DIST_DIR)/$(NAME_DIST)-$${VERSION}*.tar.gz $(RPMBUILD)/SOURCES/$(NAME)-$${VERSION}.tar.gz; \
	cp $(SPECFILE) $(RPMBUILD)/SPECS/

srpm: prepare
	@VERSION=$$(python3 -m hatchling version | sed 's/\+.*//'); \
	RELEASE=$$(python3 -m hatchling version | grep -o '+.*' | sed 's/+/./' || echo "1"); \
	rpmbuild -bs $(RPMBUILD)/SPECS/$(SPECFILE) \
		--define "_topdir $(RPMBUILD)" \
		--define "version $${VERSION}" \
		--define "release $${RELEASE}"

rpm: srpm
	@VERSION=$$(python3 -m hatchling version | sed 's/\+.*//'); \
	RELEASE=$$(python3 -m hatchling version | grep -o '+.*' | sed 's/+/./' || echo "1"); \
	rpmbuild -bb $(RPMBUILD)/SPECS/$(SPECFILE) \
		--define "_topdir $(RPMBUILD)" \
		--define "version $${VERSION}" \
		--define "release $${RELEASE}"
