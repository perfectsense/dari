MySQL plugin for manipulating JSON data.

Dependencies
============

* MySQL
* [Snappy 1.0.5](http://snappy.googlecode.com/files/snappy-1.0.5.tar.gz)
* [Jansson 2.3.1](http://www.digip.org/jansson/releases/jansson-2.3.1.tar.gz)

`Makefile` assumes these are installed in `/usr/local/`.

If you have [Homebrew](http://mxcl.github.com/homebrew/),
you can simply run:

    brew install snappy jansson

Installation
============

    make
    cp lib_mysqludf_dari.so `mysql_config --plugindir`/
    mysql -u root -p < lib_mysqludf_dari.sql
