FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN groupadd --system --gid 10001 execution \
    && useradd --system \
        --uid 10001 \
        --gid 10001 \
        --no-create-home \
        execution

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=10001:10001 execution_service ./execution_service
COPY --chown=10001:10001 --chmod=755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

USER 10001:10001

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["python", "-m", "execution_service.app"]
