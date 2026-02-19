FROM ghcr.io/prefix-dev/pixi:latest

WORKDIR /app

COPY pyproject.toml pixi.lock ./
RUN pixi install

COPY src/ ./src/

CMD ["pixi", "run", "start"]
