FROM elixir:1.14.4-slim

RUN apt-get update && apt-get install -y git
RUN apt-get update && apt-get install -y make && apt-get install -y gcc && apt-get -y install curl && apt-get -y install g++
RUN mix local.hex --force && mix local.rebar --force

COPY mix.exs mix.lock /app/

WORKDIR /app

ENV MIX_ENV=prod
RUN mix deps.get
RUN mix deps.compile --only=prod
COPY . /app
CMD mix release && _build/prod/rel/shirath/bin/shirath start