FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir . && useradd --create-home app
COPY examples ./examples
USER app
EXPOSE 4000
CMD ["uvicorn", "examples.app:app", "--host", "0.0.0.0", "--port", "4000", "--workers", "1"]
