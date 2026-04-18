NAME = gfal
NAME_DIST = gfal
SPECFILE = $(NAME).spec
DIST_DIR = dist
RPMBUILD = $(shell pwd)/rpmbuild

.PHONY: all clean dist srpm rpm prepare badges

badges:
	python3 scripts/update_badge_counts.py


clean:
	rm -rf $(DIST_DIR)
	rm -rf $(RPMBUILD)
	rm -f src/gfal/_version.py

dist: clean
	python3 -m pip install --upgrade build hatchling hatch-vcs
	python3 -m build --no-isolation

prepare: dist
	@FULL_VERSION=$$(python3 -m hatchling version); \
	VERSION=$$(echo $${FULL_VERSION} | sed 's/\+.*//'); \
	mkdir -p $(RPMBUILD)/BUILD $(RPMBUILD)/RPMS $(RPMBUILD)/SOURCES $(RPMBUILD)/SPECS $(RPMBUILD)/SRPMS; \
	cp $(DIST_DIR)/$(NAME_DIST)-$${FULL_VERSION}-py3-none-any.whl $(RPMBUILD)/SOURCES/$(NAME_DIST)-$${VERSION}-py3-none-any.whl; \
	cp $(SPECFILE) $(RPMBUILD)/SPECS/; \
	cp CHANGELOG $(RPMBUILD)/SOURCES/
	@# Generate shell completion scripts for RPM packaging
	@PYTHONPATH=src _GFAL_COMPLETE=bash_source python3 -c \
		"import sys; sys.argv=['gfal']; from gfal.cli.shell import main; main()" \
		> $(RPMBUILD)/SOURCES/gfal.bash-completion
	@PYTHONPATH=src _GFAL_COMPLETE=zsh_source python3 -c \
		"import sys; sys.argv=['gfal']; from gfal.cli.shell import main; main()" \
		> $(RPMBUILD)/SOURCES/_gfal.zsh-completion

srpm: prepare
	@FULL_VERSION=$$(python3 -m hatchling version); \
	VERSION=$$(echo $${FULL_VERSION} | sed 's/\+.*//'); \
	RELEASE=$$(echo $${FULL_VERSION} | grep -o '+.*' | sed 's/+/./'); \
	rpmbuild -bs $(RPMBUILD)/SPECS/$(SPECFILE) \
		--nodeps \
		--define "_topdir $(RPMBUILD)" \
		--define "pkg_version $${VERSION}" \
		--define "pkg_release $${RELEASE:-1}"

rpm: srpm
	@FULL_VERSION=$$(python3 -m hatchling version); \
	VERSION=$$(echo $${FULL_VERSION} | sed 's/\+.*//'); \
	RELEASE=$$(echo $${FULL_VERSION} | grep -o '+.*' | sed 's/+/./'); \
	rpmbuild -bb $(RPMBUILD)/SPECS/$(SPECFILE) \
		--nodeps \
		--define "_topdir $(RPMBUILD)" \
		--define "pkg_version $${VERSION}" \
		--define "pkg_release $${RELEASE:-1}"
