# Monobank BI

Self-hosted spending analytics for [Monobank](https://www.monobank.ua/) — a Ukrainian mobile-only bank.

Pulls transaction history via the [Monobank open API](https://api.monobank.ua/), stores it in DuckDB, and serves interactive dashboards through [Rill](https://www.rilldata.com/). Everything runs in Docker — one command to start, auto-syncs every hour.

### What it does

- Syncs all accounts (UAH, USD, EUR) with incremental updates
- Categorizes transactions by MCC codes (groceries, restaurants, transport, etc.)
- Provides explore dashboards: filter by category, merchant, account, time period
- Exports to Parquet for portability

### Stack

- **Sync**: Python + Monobank API + DuckDB
- **BI**: Rill (DuckDB-native, lightweight)
- **Infra**: Docker Compose, cron for auto-sync


## Local Setup

```bash
cp .env.example .env
# Set MONO_TOKEN (get it at https://api.monobank.ua/)

docker compose up -d --build
# Rill: http://localhost:9010
```

## Project Structure

```
monoapi/
  sync.py              # Monobank API → DuckDB → Parquet
  Dockerfile.sync      # Sync container with cron
  entrypoint.sh        # Initial sync + hourly cron job
  docker-compose.yml   # sync + rill services
  rill/
    rill.yaml           # Rill config
    connectors/         # DuckDB connector
    models/
      transactions.sql  # SQL model: MCC categories, merchants, day of week
    metrics/
      spending.yaml     # Measures: expenses, income, net, avg check, cashback
    dashboards/
      overview.yaml     # Explore dashboard
```

---

## AWS Deployment

### Option A: EC2 (simplest)

Single t3.micro (free tier eligible), same docker-compose setup.

#### 1. Create EC2 Instance

```bash
# Amazon Linux 2023 / Ubuntu 22.04, t3.micro
# Security group: open ports 22 (SSH), 443 (HTTPS)
```

#### 2. Install Docker

```bash
# Amazon Linux
sudo yum install -y docker
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user

# Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

#### 3. Deploy

```bash
# Copy project to server
scp -r . ec2-user@<EC2-IP>:~/monoapi

# On the server
ssh ec2-user@<EC2-IP>
cd ~/monoapi
echo "MONO_TOKEN=<your-token>" > .env
docker compose up -d --build
```

#### 4. HTTPS + Basic Auth (Caddy)

Add to `docker-compose.yml`:

```yaml
  caddy:
    image: caddy:2
    ports:
      - "443:443"
      - "80:80"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile
      - caddy-data:/data
    depends_on:
      - rill
```

Create `Caddyfile`:

```
your-domain.com {
    basicauth {
        anton <bcrypt-password-hash>
    }
    reverse_proxy rill:9009
}
```

#### 5. Cost

| Resource | Price |
|---|---|
| t3.micro (free tier, 1 year) | $0 |
| t3.micro (after free tier) | ~$8/mo |
| EBS 8GB | ~$0.80/mo |
| Elastic IP | free while attached |
| **Total** | **$0 — $9/mo** |

---

### Option B: ECS Fargate (serverless, pricier)

Containers without managing EC2. Pay per CPU/RAM usage.

#### Architecture

```
                    ┌──────────────┐
                    │   ECS Task   │
                    │  ┌────────┐  │
Monobank API ◄─────┤  │ sync   │  │
                    │  └───┬────┘  │
                    │      │       │
                    │  ┌───▼────┐  │
                    │  │  EFS   │  │ (shared /data)
                    │  └───┬────┘  │
                    │      │       │
                    │  ┌───▼────┐  │
Internet ──► ALB ──┤  │  rill  │  │
                    │  └────────┘  │
                    └──────────────┘
```

#### Steps

1. Create ECR repository, push sync image
2. Create EFS for `/data` (parquet + duckdb)
3. Create ECS Cluster (Fargate)
4. Task Definition: 2 containers (sync + rill), mount EFS
5. ALB + Target Group on port 9009
6. Route 53 for custom domain (optional)

#### Cost

| Resource | Price |
|---|---|
| Fargate (0.25 vCPU, 0.5GB) 24/7 | ~$9/mo |
| EFS | ~$0.30/mo |
| ALB | ~$16/mo |
| **Total** | **~$25/mo** |

---

### Option C: Lightsail (middle ground)

AWS Lightsail = simplified VPS with fixed pricing.

```bash
# Create Lightsail instance (via AWS console)
# OS: Ubuntu 22.04, Plan: $3.50/mo (512MB) or $5/mo (1GB)

# SSH and deploy same as Option A
ssh ubuntu@<lightsail-ip>
# ... docker compose up -d
```

| Resource | Price |
|---|---|
| Lightsail 1GB RAM | $5/mo |
| Static IP | free |
| **Total** | **$5/mo** |

---

## Recommendation

**For a single user — Option A (EC2 t3.micro)** or **Option C (Lightsail $5/mo)**.

Fargate only makes sense if you need auto-scaling or want zero server management, but the ALB triples the cost.

## Quick Deploy (copy-paste)

```bash
# 1. Local machine
tar czf monoapi.tar.gz --exclude=venv --exclude=.git --exclude='*.pyc' .
scp monoapi.tar.gz ubuntu@<SERVER-IP>:~

# 2. On the server
ssh ubuntu@<SERVER-IP>
mkdir -p ~/monoapi && cd ~/monoapi
tar xzf ~/monoapi.tar.gz
echo "MONO_TOKEN=<token>" > .env
docker compose up -d --build

# Done: http://<SERVER-IP>:9010
```
