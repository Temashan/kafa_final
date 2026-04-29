FROM python:3.11-slim

WORKDIR /app

ENV PYTHONPATH=/app

RUN apt-get update && apt-get install -y \
    build-essential \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock* ./

RUN pip install uv
RUN uv sync

COPY . .

CMD ["sh", "-c", "PYTHONPATH=/app exec uv run python -m services.product_filter_service.streaming.app worker -l info --without-web"]
