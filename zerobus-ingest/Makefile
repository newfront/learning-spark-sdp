.PHONY: build descriptor generate

# Lint, format, and generate language bindings (e.g. Python).
build:
	buf lint && buf format -w && buf generate

# Build protos then run the app in generate mode to create 100 orders.
generate: build
	uv run main.py --generate --count 100

# Generate a serialized FileDescriptorSet for use in the Apache Spark application
# (read/ingest orders). Output is Protobuf binary format; --exclude-source-info
# keeps the file smaller. See: https://buf.build/docs/reference/descriptors/
descriptor:
	buf build -o descriptor.bin --exclude-source-info
