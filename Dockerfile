FROM ghcr.io/prefix-dev/pixi:latest

WORKDIR /app

COPY pyproject.toml ./
COPY src/ ./src/

RUN pixi install

CMD ["pixi", "run", "start"]
