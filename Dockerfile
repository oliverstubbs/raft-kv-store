FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Build the venv outside /app
ENV UV_PROJECT_ENVIRONMENT=/opt/venv

# so print() is shown in logs
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY . .

CMD ["uv", "run", "uvicorn", "server:server", "--host", "0.0.0.0", "--port", "8000"]
