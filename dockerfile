FROM python:3.10-slim as builder

RUN apt-get update && apt-get install -y curl build-essential

WORKDIR /app

COPY pyproject.toml poetry.lock* /app/

RUN curl -sSL https://install.python-poetry.org | python3 -

ENV PATH="/root/.local/bin:${PATH}"

RUN poetry self add poetry-plugin-export

RUN poetry export -f requirements.txt --without-hashes -o requirements.txt

FROM python:3.10-slim

RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

RUN ls -la

COPY . /app

CMD ["python", "pipeline/etl.py"]
