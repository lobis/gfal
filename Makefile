NAME := gfal
SPECFILE := $(NAME).spec
DIST_DIR := dist
RPMBUILD := $(CURDIR)/rpmbuild
PYTHON ?= python3
REMOTE ?= lobis-eos-dev
REF ?= HEAD

.PHONY: all clean dist srpm rpm prepare deploy-rpm

all: dist

clean:
	rm -rf $(DIST_DIR) $(RPMBUILD)
	rm -f src/gfal/_version.py

dist: clean
	$(PYTHON) -m build --no-isolation

prepare: dist
	@FULL_VERSION=$$($(PYTHON) -c 'from pathlib import Path; print(next(Path("dist").glob("gfal-*.tar.gz")).name.removeprefix("gfal-").removesuffix(".tar.gz"))'); \
	mkdir -p $(RPMBUILD)/BUILD $(RPMBUILD)/BUILDROOT $(RPMBUILD)/RPMS $(RPMBUILD)/SOURCES $(RPMBUILD)/SPECS $(RPMBUILD)/SRPMS; \
	cp "$(DIST_DIR)/$(NAME)-$${FULL_VERSION}.tar.gz" "$(RPMBUILD)/SOURCES/$(NAME)-$${FULL_VERSION}.tar.gz"; \
	cp $(SPECFILE) $(RPMBUILD)/SPECS/

srpm: prepare
	@FULL_VERSION=$$($(PYTHON) -c 'from pathlib import Path; print(next(Path("dist").glob("gfal-*.tar.gz")).name.removeprefix("gfal-").removesuffix(".tar.gz"))'); \
	VERSION=$${FULL_VERSION%%+*}; \
	LOCAL=$${FULL_VERSION#$$VERSION}; LOCAL=$${LOCAL#+}; \
	RELEASE=1$${LOCAL:+.$$LOCAL}; \
	rpmbuild -bs $(RPMBUILD)/SPECS/$(SPECFILE) \
		--define "_topdir $(RPMBUILD)" \
		--define "pkg_version $$VERSION" \
		--define "pkg_release $$RELEASE" \
		--define "source_version $$FULL_VERSION"

rpm: prepare
	@FULL_VERSION=$$($(PYTHON) -c 'from pathlib import Path; print(next(Path("dist").glob("gfal-*.tar.gz")).name.removeprefix("gfal-").removesuffix(".tar.gz"))'); \
	VERSION=$${FULL_VERSION%%+*}; \
	LOCAL=$${FULL_VERSION#$$VERSION}; LOCAL=$${LOCAL#+}; \
	RELEASE=1$${LOCAL:+.$$LOCAL}; \
	rpmbuild -ba $(RPMBUILD)/SPECS/$(SPECFILE) \
		--define "_topdir $(RPMBUILD)" \
		--define "pkg_version $$VERSION" \
		--define "pkg_release $$RELEASE" \
		--define "source_version $$FULL_VERSION"

deploy-rpm:
	./scripts/deploy-el-rpm.sh "$(REMOTE)" "$(REF)"
