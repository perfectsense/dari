all: lib_mysqludf_dari.so

lib_mysqludf_dari.so: lib_mysqludf_dari.c
	gcc -O3 -Wall -ljansson -lsnappy -I/usr/local/include -I/usr/local/mysql/include -I. -shared lib_mysqludf_dari.c -o lib_mysqludf_dari.so

install: all
	cp lib_mysqludf_dari.so /usr/local/mysql/lib/plugin

clean: 
	rm -rf lib_mysqludf_dari.so lib_mysqludf_dari.so.dSYM
