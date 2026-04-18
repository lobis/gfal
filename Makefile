NAME = gfal
NAME_DIST = gfal
SPECFILE = $(NAME).spec
DIST_DIR = dist
RPMBUILD = $(shell pwd)/rpmbuild
LXPLUS_HOST ?= lxplus

.PHONY: all clean dist srpm rpm prepare badges deploy-lxplus

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

deploy-lxplus:
	@set -e; \
	BRANCH_NAME=$$(git rev-parse --abbrev-ref HEAD); \
	BRANCH_SAFE=$$(printf '%s' "$$BRANCH_NAME" | tr '/:' '--'); \
	REMOTE_DIR="tmp/$(NAME)-$$BRANCH_SAFE"; \
	ssh "$(LXPLUS_HOST)" "mkdir -p \"\$$HOME/$$REMOTE_DIR\""; \
	echo "Deploying $$BRANCH_NAME to $(LXPLUS_HOST):~/$$REMOTE_DIR"; \
	rsync -az --delete \
		--exclude '.venv' \
		--exclude '.mypy_cache' \
		--exclude '.pytest_cache' \
		--exclude '.ruff_cache' \
		--exclude 'dist' \
		--exclude 'rpmbuild' \
		./ "$(LXPLUS_HOST):$$REMOTE_DIR/"; \
	ssh "$(LXPLUS_HOST)" "set -e; \
		cd \"\$$HOME/$$REMOTE_DIR\"; \
		python3 -m venv .venv; \
		. .venv/bin/activate; \
		python -m pip install --upgrade pip >/dev/null; \
		python -m pip install -e .; \
		python -c \"import gfal; print(gfal.__file__)\"; \
		gfal version"; \
	echo ""; \
	echo "Environment ready on $(LXPLUS_HOST)."; \
	echo "Load it with:"; \
	echo "  ssh $(LXPLUS_HOST) 'cd \$$HOME/$$REMOTE_DIR && . .venv/bin/activate && exec \$$SHELL -l'"
