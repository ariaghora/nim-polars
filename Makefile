OUT=target/release/libnim_polars.dylib
LIBDIR=/usr/local/lib/

all: ${OUT}
	@cargo build -r

install:
	@nimble install -y
	@cp ${OUT} ${LIBDIR}

test: all
	@nimble test