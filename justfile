set windows-shell := ["cmd.exe", "/c"]

default:
    just --list

check:
    cargo clippy --workspace --all-targets

test:
    cargo test -p abyss-core -p abyss-collect --lib

up:
    docker compose up -d

down:
    docker compose down

migrate:
    cargo run -p abyss -- migrate

seed:
    cargo run -p abyss -- seed

crawl:
    cargo run -p abyss --profile quick -- crawl

aggregate:
    cargo run -p abyss -- aggregate

serve: up
    cargo run -p abyss-serve --profile quick

stats:
    cargo run -p abyss -- stats
