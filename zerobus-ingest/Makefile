# Buf index required for bufbuild-protovalidate-protocolbuffers-python (not on PyPI).
BUF_INDEX = https://buf.build/gen/python

.PHONY: build descriptor generate install-dist package publish release test

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

# Bump the project version in pyproject.toml.
# Usage: make release [bump=minor]
#   bump: major | minor | patch | stable | alpha | beta | rc | post | dev (default: minor)
# Examples:
#   make release           # 0.1.0 -> 0.2.0 (minor)
#   make release bump=patch # 0.1.0 -> 0.1.1
#   make release bump=major # 0.1.0 -> 1.0.0
release:
	uv version --bump $(or $(bump),minor)

# Install from dist/ (wheel or sdist) with extra index for Buf deps.
# Run 'make package' first. Use when installing the built artifact elsewhere.
install-dist:
	@if [ -n "$$(find dist -maxdepth 1 -name '*.whl' -print -quit)" ]; then \
		uv pip install --extra-index-url $(BUF_INDEX) dist/*.whl; \
	elif [ -n "$$(find dist -maxdepth 1 -name '*.tar.gz' -print -quit)" ]; then \
		uv pip install --extra-index-url $(BUF_INDEX) dist/*.tar.gz; \
	else \
		echo "No wheel or sdist in dist/. Run 'make package' first."; exit 1; \
	fi

# Package the application: build wheel and sdist into dist/.
# See: https://docs.astral.sh/uv/guides/package/
package:
	uv build

# Publish the package to PyPI (or UV_PUBLISH_INDEX).
# Requires UV_PUBLISH_TOKEN (PyPI API token) or UV_PUBLISH_USERNAME + UV_PUBLISH_PASSWORD.
# Example: UV_PUBLISH_TOKEN=pypi-xxx make publish
publish: package
	uv publish
