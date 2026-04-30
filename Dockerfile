FROM python:3.13-slim
WORKDIR /app/src

COPY ./src .
ENV DATA_DIR=/app/data
ENTRYPOINT ["python", "node.py"]