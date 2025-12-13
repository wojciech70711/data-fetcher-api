# Running get_data_from_market.py in Docker

Build the image:

```bash
docker build -t data-fetcher:latest .
```

Run the container (example):

```bash
docker run --rm -v "$(pwd)/logs:/app/logs" -v "$(pwd)/data:/app/data" data-fetcher:latest --pairs BTC ETH --interval 1h --limit 10
```

Or use docker-compose:

```bash
docker-compose up --build
```

Notes:
- The container uses Python 3.11-slim as base.
- Logs are written to `/app/logs` and bound to the host `./logs`.
- Data (CSV) is stored in `/app/data` and bound to the host `./data`.
- Override the default CLI arguments by passing your own after the image name.
