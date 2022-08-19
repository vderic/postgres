TOPDIR = ..
JSONTAR = json-c.tar.gz
JSONDIR = $(JSONTAR:.tar.gz=)
XRGTAR = xrg-r220809.zip
XRGDIR = $(XRGTAR:.zip=)
ARROWZIP = apache-arrow-7.0.0-static.zip
ARROWDIR = $(ARROWZIP:.zip=)
SOCKSTREAMTAR = sockstream-r220819.zip
SOCKSTREAMDIR = $(SOCKSTREAMTAR:.zip=)

all: $(SUBDIR) include/xrg.h include/arrow/api.h include/json-c/json.h include/sockstream.h

$(SUBDIR):
	mkdir $@

include/json-c/json.h : tarball/$(JSONTAR)
	rm -rf $(JSONDIR)
	tar zxvf tarball/$(JSONTAR)
	(cd json-c && mkdir -p build && cd build && ../cmake-configure --disable-shared --prefix=../../ && make && make install)

include/xrg.h: tarball/$(XRGTAR)
	rm -rf $(XRGDIR)
	unzip -q tarball/$(XRGTAR)
	make -C $(XRGDIR) install prefix=$(shell pwd)

include/arrow/api.h: tarball/$(ARROWZIP)
	rm -rf $(ARROWDIR)
	unzip -q tarball/$(ARROWZIP)
	cp -r $(ARROWDIR)/include/* include
	cp -r $(ARROWDIR)/lib/* lib

include/sockstream.h : tarball/$(SOCKSTREAMTAR)
	rm -rf $(SOCKSTREAMDIR)
	unzip -q tarball/$(SOCKSTREAMTAR)
	make -C $(SOCKSTREAMDIR) install prefix=$(shell pwd)

wipe:
	rm -rf bin include lib share $(XRGDIR) $(JSONDIR) $(ARROWDIR) $(SOCKSTREAMDIR)
