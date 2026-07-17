# --- stage 1: build the wheels and generate the protobuf stubs -------------
FROM python:3.12-slim AS build

WORKDIR /build
RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt ./
RUN pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt

# The generated *_pb2.py files are BUILD OUTPUT, not source. Generating them
# here means the .proto is the single source of truth and the stubs can never
# drift from it — which is exactly the bug you get when you commit them.
COPY proto ./proto
RUN pip install --no-cache-dir grpcio-tools==1.66.1 \
    && python -m grpc_tools.protoc \
        -I. \
        --python_out=. \
        --grpc_python_out=. \
        proto/pricing.proto \
    && touch proto/__init__.py

# --- stage 2: the runtime image -------------------------------------------
FROM python:3.12-slim AS runtime

# Run as a non-root user. A container that is root is one container-escape CVE
# away from being root on the node, and nothing here needs the privilege.
RUN useradd --create-home --uid 10001 app
WORKDIR /app

COPY --from=build /wheels /wheels
COPY requirements.txt ./
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt \
    && rm -rf /wheels

COPY --from=build /build/proto ./proto
COPY dispatch ./dispatch
COPY pricing ./pricing
COPY schema.sql ./schema.sql

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
USER app

# No CMD. This image has three entrypoints (api, matcher, pricing) and the
# orchestrator chooses which one to run. One artifact, one build, one SHA to
# roll back — three roles. See k8s/ in the next step.
