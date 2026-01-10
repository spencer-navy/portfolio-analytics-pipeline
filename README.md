# Portfolio Analytics Pipeline

**Production-grade event tracking and analytics infrastructure**

Built to showcase end-to-end data engineering skills for ML Engineering roles.

---

## 📊 Architecture Overview

```
User Interaction → MongoDB → Batch Processor → Redis Cache → Dashboard
                      ↓                ↓              ↓
                Raw Events    Clean & Transform   Pre-aggregated
                                                    Metrics
```

### Components

1. **MongoDB Atlas** - Stores raw event data from portfolio website
2. **Batch Processor** - Python script that cleans, transforms, and aggregates data
3. **Redis Cache** - Stores pre-computed metrics for fast dashboard queries
4. **GitHub Actions** - Scheduled automation (runs processor every hour)
5. **Plotly Dash** - Interactive analytics dashboard (optional)

---

## 🎯 What This Demonstrates

### Data Engineering Skills
- ETL pipeline design and implementation
- NoSQL database integration (MongoDB)
- In-memory caching strategies (Redis)
- Data cleaning and feature engineering
- Batch processing architecture

### Software Engineering Skills
- Clean, documented, production-ready code
- Docker containerization
- CI/CD with GitHub Actions
- Error handling and logging
- Environment management

### ML Engineering Skills
- Feature engineering for analytics/ML
- Data pipeline optimization
- Scalable architecture design
- Privacy-conscious data handling (IP hashing, GDPR)

---

## 🚀 Quick Start

### Prerequisites

```bash
# Required
Python 3.11+
MongoDB Atlas account (free tier)
Redis instance (free: Redis Cloud, Render, Railway)

# Optional (for Docker)
Docker Desktop
```

### 1. Clone and Setup

```bash
# Clone repository
git clone <your-repo-url>
cd portfolio-analytics

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Create a `.env` file:

```bash
# .env
MONGO_URI=mongodb+srv://username:password@cluster.mongodb.net/portfolio
REDIS_URL=redis://default:password@redis-host:6379
```

**Get your MongoDB URI:**
1. Go to MongoDB Atlas → Database → Connect
2. Choose "Connect your application"
3. Copy the connection string
4. Replace `<password>` with your database password

**Get your Redis URL:**
1. Sign up for Redis Cloud (free 30MB tier)
2. Create database
3. Copy the connection string from database details

### 3. Test Locally

```bash
# Run batch processor once
python batch_processor.py

# Expected output:
# ============================================================
# STARTING BATCH EVENT PROCESSING JOB
# ============================================================
# Connecting to MongoDB...
# ✓ Connected to MongoDB (Database: portfolio)
# Connecting to Redis...
# ✓ Connected to Redis
# Loading events from MongoDB...
# ...
```

---

## 🐳 Docker Usage

### Why Docker?

Docker packages your application with all dependencies into a container that runs identically everywhere. It's like shipping a fully-configured computer instead of just code.

**Benefits:**
- ✅ No "works on my machine" issues
- ✅ Same environment: dev, testing, production
- ✅ Easy deployment to cloud platforms
- ✅ Industry-standard skill

### Docker Basics Tutorial

```bash
# 1. BUILD: Create a Docker image from Dockerfile
docker build -t portfolio-analytics .

# What happens:
# - Docker reads the Dockerfile line-by-line
# - Creates layers (cached for speed)
# - Installs Python, dependencies, copies code
# - Result: A packaged image ready to run

# 2. RUN: Start a container from the image
docker run portfolio-analytics

# What happens:
# - Docker creates a fresh container from the image
# - Runs the CMD command (batch_processor.py)
# - Container exits when script finishes

# 3. RUN with environment variables
docker run \
  -e MONGO_URI="your-mongo-uri" \
  -e REDIS_URL="your-redis-url" \
  portfolio-analytics

# 4. RUN interactively (useful for debugging)
docker run -it portfolio-analytics /bin/bash
# Now you're inside the container!
# Try: python batch_processor.py

# 5. RUN dashboard (if you build one)
docker run -p 8050:8050 portfolio-analytics python dashboard.py
# Access at http://localhost:8050

# 6. VIEW running containers
docker ps

# 7. VIEW all containers (including stopped)
docker ps -a

# 8. STOP a running container
docker stop <container-id>

# 9. REMOVE a container
docker rm <container-id>

# 10. VIEW images on your machine
docker images

# 11. REMOVE an image
docker rmi portfolio-analytics
```

### Docker Compose (Multiple Services)

If you want to run multiple containers together (app + Redis):

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
  
  processor:
    build: .
    environment:
      - MONGO_URI=${MONGO_URI}
      - REDIS_URL=redis://redis:6379
    depends_on:
      - redis
```

```bash
# Start all services
docker-compose up

# Run in background
docker-compose up -d

# Stop all services
docker-compose down
```

---

## ⏰ Automated Scheduling

### GitHub Actions (Free, Recommended)

GitHub Actions runs your code on GitHub's servers automatically.

**Setup:**

1. Create `.github/workflows/hourly_processing.yml` (already included)

2. Add secrets to GitHub:
   - Go to: Repository → Settings → Secrets and variables → Actions
   - Click "New repository secret"
   - Add `MONGO_URI` with your MongoDB connection string
   - Add `REDIS_URL` with your Redis connection string

3. Push to GitHub:
   ```bash
   git add .
   git commit -m "Add analytics pipeline"
   git push origin main
   ```

4. Monitor:
   - Go to "Actions" tab in your GitHub repo
   - See workflow runs, logs, and artifacts

**Manual Trigger:**
- Go to Actions → Hourly Event Processing → Run workflow

**Cost:** FREE (2,000 minutes/month, this uses ~1,500/month)

### Alternative: AWS Lambda

For production deployments, you might use AWS Lambda:

```python
# lambda_function.py
import json
from batch_processor import BatchProcessor
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """AWS Lambda entry point"""
    processor = BatchProcessor()
    
    # Process last hour
    start = datetime.utcnow() - timedelta(hours=1)
    df = processor.load_events(start_date=start)
    
    cleaned = processor.clean_dataframe(df)
    featured = processor.add_features(cleaned)
    processor.update_redis_cache(featured)
    
    processor.close()
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Processed {len(featured)} events')
    }
```

**Deploy to Lambda:**
```bash
# Package dependencies
pip install -r requirements.txt -t package/
cp batch_processor.py package/
cp lambda_function.py package/
cd package && zip -r ../lambda-deployment.zip . && cd ..

# Upload to Lambda via AWS Console or CLI
aws lambda update-function-code \
  --function-name portfolio-analytics \
  --zip-file fileb://lambda-deployment.zip
```

---

## 📈 Data Flow Explained

### Step-by-Step: What Happens Every Hour

```
1. TRIGGER
   GitHub Actions cron: '0 * * * *' fires
   ↓

2. SETUP
   GitHub spins up Ubuntu VM
   Installs Python 3.11
   Installs dependencies from requirements.txt
   ↓

3. CONNECT
   batch_processor.py connects to:
   - MongoDB Atlas (raw events)
   - Redis (cache)
   ↓

4. EXTRACT
   Loads last hour of events from MongoDB
   Query: { timestamp: { $gte: start, $lte: end } }
   ↓

5. CLEAN
   - Remove duplicates
   - Handle missing values (referrer → 'direct')
   - Parse user agents (device type, browser)
   - Hash IP addresses (privacy)
   - Flatten metadata into columns
   ↓

6. TRANSFORM
   - Extract time features (hour, day_of_week)
   - Calculate session metrics (duration, event count)
   - Add engagement flags (quick_exit, deep_engagement)
   - Compute derived features
   ↓

7. AGGREGATE
   Update Redis with pre-computed metrics:
   - Total event counts by type
   - Events per minute (time series)
   - Top 10 filters (sorted set)
   - Top 10 projects (sorted set)
   - Session statistics (average duration)
   ↓

8. SAVE
   Optionally save cleaned data:
   - Parquet file → S3 (future use)
   - Local file → GitHub Actions artifact
   ↓

9. DASHBOARD READS
   Dashboard queries Redis (fast!), not MongoDB
   Redis response time: <1ms vs MongoDB: 50-100ms
   ↓

10. CLEANUP
    Close connections
    VM shuts down
    Wait 60 minutes, repeat
```

### Why This Architecture?

**Separation of Concerns:**
- MongoDB = Long-term storage, source of truth
- Batch Processor = Data cleaning and transformation
- Redis = Speed layer for real-time queries
- Dashboard = Visualization, no heavy computation

**Performance:**
- Pre-aggregation in batch job (heavy lifting once per hour)
- Dashboard reads cached data (instant response)
- Users never wait for data processing

**Scalability:**
- Add more batch workers (process different time ranges in parallel)
- Redis handles millions of requests/second
- MongoDB sharding for billions of events

---

## 📂 Project Structure

```
portfolio-analytics/
├── batch_processor.py          # Main data processing script
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Container definition
├── .github/
│   └── workflows/
│       └── hourly_processing.yml  # GitHub Actions automation
├── .env.example                # Template for environment variables
├── README.md                   # This file
├── tests/                      # Unit tests (TODO)
│   └── test_processor.py
└── docs/
    ├── DOCKER_TUTORIAL.md      # Detailed Docker guide
    └── MONGODB_SCHEMA.md       # Event schema documentation
```

---

## 🔍 Monitoring and Debugging

### View GitHub Actions Logs

1. Go to repository → Actions tab
2. Click on a workflow run
3. Click on job name ("process-events")
4. Expand steps to see detailed logs

### Common Issues

**"No events to process"**
- ✅ Normal if website had no traffic that hour
- Check MongoDB directly to verify events exist

**"Failed to connect to MongoDB"**
- ❌ Check `MONGO_URI` secret in GitHub
- ❌ Check MongoDB Atlas network access (allow GitHub IPs)
- ❌ Verify connection string is correct

**"Redis connection failed"**
- ❌ Check `REDIS_URL` secret
- ❌ Verify Redis instance is running
- ⚠️  Pipeline continues without Redis (caching disabled)

### Test Locally First

```bash
# Set environment variables
export MONGO_URI="your-uri"
export REDIS_URL="your-redis-url"

# Run with verbose logging
python batch_processor.py

# Or use .env file
python -c "from dotenv import load_dotenv; load_dotenv(); import batch_processor; batch_processor.main()"
```

---

## 🎓 Learning Resources

### Docker
- [Docker Getting Started](https://docs.docker.com/get-started/)
- [Docker Best Practices](https://docs.docker.com/develop/dev-best-practices/)
- [Play with Docker](https://labs.play-with-docker.com/) - Free online playground

### GitHub Actions
- [GitHub Actions Quickstart](https://docs.github.com/en/actions/quickstart)
- [Workflow Syntax](https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions)
- [Cron Schedule Examples](https://crontab.guru/)

### MongoDB
- [MongoDB Python Driver Tutorial](https://pymongo.readthedocs.io/en/stable/tutorial.html)
- [Aggregation Pipeline](https://www.mongodb.com/docs/manual/aggregation/)

### Redis
- [Redis Python Client](https://redis-py.readthedocs.io/en/stable/)
- [Redis Data Types](https://redis.io/docs/data-types/)

---

## 🚧 Future Enhancements

### Phase 1: Current (✅ Complete)
- [x] MongoDB event tracking
- [x] Batch processing pipeline
- [x] Redis caching
- [x] GitHub Actions automation
- [x] Docker containerization

### Phase 2: Analytics Dashboard (🔄 In Progress)
- [ ] Plotly Dash interactive dashboard
- [ ] Real-time metrics visualization
- [ ] User journey funnel analysis
- [ ] Geographic heatmap

### Phase 3: Advanced Features (📋 Planned)
- [ ] Anomaly detection (traffic spikes, unusual patterns)
- [ ] A/B testing framework
- [ ] Recommendation engine (collaborative filtering)
- [ ] Predictive analytics (time series forecasting)

### Phase 4: Production Scale (🎯 Future)
- [ ] Apache Airflow orchestration
- [ ] AWS Lambda deployment
- [ ] S3 data lake integration
- [ ] Real-time streaming (MongoDB Change Streams)
- [ ] Machine learning model deployment

---

## License

MIT License - Feel free to use this for your own portfolio!

---

## Author

**Abigail Spencer**
- Portfolio: [abigailspencer.dev](https://abigailspencer.dev)
- LinkedIn: [Your LinkedIn]
- GitHub: [@YourGitHub]

Built as part of ML Engineering portfolio to demonstrate:
- Production data pipeline design
- Cloud-native architecture
- DevOps best practices
- Real-world problem solving

---

## Acknowledgments

Inspired by industry best practices from:
- Airbnb's data pipeline architecture
- Netflix's event processing system
- Spotify's recommendation engine design

Special thanks to the open-source community for amazing tools!