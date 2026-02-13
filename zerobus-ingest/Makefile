.PHONY: build descriptor generate test

# Lint, format, and generate language bindings (protos + Python).
build:
	buf lint && buf format -w && buf generate
	uv run ruff format src tests main.py 
	uv run ruff check src tests main.py --fix

# Build protos then run the app in generate mode to create 100 orders.
generate: build
	uv run main.py --generate --count 100

# Run tests with pytest.
test:
	uv run pytest

# Generate a serialized FileDescriptorSet for use in the Apache Spark application
# (read/ingest orders). Output is Protobuf binary format; --exclude-source-info
# keeps the file smaller. See: https://buf.build/docs/reference/descriptors/
descriptor:
	buf build -o gen/python/orders/v1/descriptor.bin --exclude-source-info
