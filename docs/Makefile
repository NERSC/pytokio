.PHONY: clean

SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = pytokio
SPHINXBUILDER = html
SOURCEDIR     = source
BUILDDIR      = build
TARGETS       = $(BUILDDIR)/html/index.html \
			    $(BUILDDIR)/html/modules.html \
			    $(BUILDDIR)/html/tokio.connectors.html \
			    $(BUILDDIR)/html/tokio.html \
			    $(BUILDDIR)/html/tokio.tools.html \
			    $(BUILDDIR)/html/tokiobin.html

$(TARGETS):

$(BUILDDIR)/html/%.html: source/%.rst
	$(SPHINXBUILD) -M $(SPHINXBUILDER) "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

source/tokio.rst:
	sphinx-apidoc -o source/ ../tokio/

#source/tokiobin.rst:
#	sphinx-apidoc -o source/ ../tests/tokiobin/

clean:
	rm -rf $(BUILDDIR)
