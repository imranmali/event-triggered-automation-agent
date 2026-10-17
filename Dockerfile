# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS build
WORKDIR /src
RUN pip install --no-cache-dir build
COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m build --wheel --outdir /dist

FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ETA_CONFIG_DIR=/app/config \
    ETA_HOST=0.0.0.0 \
    ETA_PORT=8080
RUN useradd --system --uid 10001 --home /app eta
WORKDIR /app
COPY --from=build /dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl
COPY config ./config
USER eta
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz').status==200 else 1)"
# Same image runs every role: `serve`, `worker`, `consume`, `db upgrade`.
ENTRYPOINT ["eta"]
CMD ["serve"]
