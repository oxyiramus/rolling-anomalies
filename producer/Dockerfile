FROM python:3.14-slim AS compile-image

WORKDIR /code

COPY ./producer/pyproject.toml ./producer/main.py ./

RUN \
    pip install --upgrade pip && \
    pip wheel --no-cache-dir --wheel-dir /wheels .

FROM python:3.14-slim AS build-image

RUN useradd --system zhdanova

WORKDIR /code

COPY --from=compile-image /wheels /wheels

RUN \
    pip install --upgrade pip && \
    pip install --no-cache-dir /wheels/*

USER zhdanova

CMD ["python", "-m", "main"]
