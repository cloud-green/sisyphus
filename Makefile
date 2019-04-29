SISYPHUS_VERSION := $(shell awk '/version:/ {print $$2}' snap/snapcraft.yaml | head -1 | sed "s/'//g")

.PHONY: all
all: snap lint charm

.PHONY: lint
lint:
	flake8 --ignore=E121,E123,E126,E226,E24,E704,E265 charm/sisyphus

.PHONY: snap
snap: sisyphus_$(SISYPHUS_VERSION)_amd64.snap

sisyphus_$(SISYPHUS_VERSION)_amd64.snap:
	snapcraft

.PHONY: charm
charm: charm/builds/sisyphus

charm/builds/sisyphus:
	$(MAKE) -C charm/sisyphus

.PHONY: clean-charm
clean-charm:
	$(RM) -r charm/builds charm/deps

.PHONY: clean-snap
clean-snap:
	$(RM) sisyphus_$(SISYPHUS_VERSION)_amd64.snap
	snapcraft clean

.PHONY: clean
clean: clean-charm clean-snap