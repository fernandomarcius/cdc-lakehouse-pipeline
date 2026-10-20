FROM python:3.12-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PORT=8080
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY sink ./sink
COPY worker ./worker

# Any non-root uid works: the container only writes to /tmp.
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["python", "/app/worker/cycle.py"]
