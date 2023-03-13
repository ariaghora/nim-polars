OUT=target/release/libnim_polars.dylib
LIBDIR=/usr/local/lib/

all: 
	@cargo build -r

install:
	@nimble install -y
	@cp ${OUT} ${LIBDIR}

test: all
	@nimble test