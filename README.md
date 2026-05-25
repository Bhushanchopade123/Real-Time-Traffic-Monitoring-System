# Real-Time Traffic Monitoring System

A comprehensive big data pipeline for real-time taxi fleet monitoring using Apache Kafka, Apache Flink, Redis, and Docker.

## 📋 Project Overview

This system simulates live taxi movement data and processes it in real-time to detect events such as:
- **Speeding taxis** (exceeding speed limits)
- **Taxis leaving defined areas** (geofencing violations)
- **Average speed calculations**
- **Distance traveled tracking**
- **Live dashboard visualization**

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Message Broker | Apache Kafka | Real-time data streaming |
| Stream Processing | Apache Flink | Complex event processing |
| Data Cache | Redis | Storing processed results |
| Containerization | Docker & Docker Compose | Service orchestration |
| Data Simulation | Python | Generating taxi movement data |
| Frontend | React + D3.js | Live dashboard |
| Backend API | Python Flask | REST API for dashboard |

## 📁 Project Structure

```
Real-Time-Traffic-Monitoring-System/
├── docker-compose.yml          # Docker service configuration
├── README.md                   # This file
├── requirements.txt            # Python dependencies
│
├── kafka/                      # Kafka configuration
│   └── kafka-topics-init.sh   # Kafka topic initialization
│
├── flink/                      # Apache Flink jobs
│   ├── Dockerfile             # Flink Docker image
│   └── taxi_monitoring.py      # Main Flink streaming job
│
├── data-simulator/            # Taxi data simulator
│   ├── Dockerfile             # Simulator Docker image
│   ├── taxi_simulator.py       # Data generator
│   └── taxi_config.py          # Configuration
│
├── api-server/                # Flask backend API
│   ├── Dockerfile             # API Docker image
│   ├── app.py                 # Main Flask app
│   └── redis_client.py         # Redis connection handler
│
├── dashboard/                 # React frontend
│   ├── public/
│   ├── src/
│   │   ├── components/
│   │   ├── App.js
│   │   └── index.js
│   └── package.json
│
└── scripts/                   # Utility scripts
    ├── setup.sh               # Initial setup
    └── cleanup.sh             # Cleanup script
```

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose installed
- Python 3.8+
- Node.js 14+
- Git

### Step 1: Clone the Repository
```bash
git clone https://github.com/Bhushanchopade123/Real-Time-Traffic-Monitoring-System.git
cd Real-Time-Traffic-Monitoring-System
```

### Step 2: Start Services with Docker Compose
```bash
docker-compose up -d
```

This will start:
- Kafka broker
- Zookeeper
- Flink JobManager & TaskManager
- Redis
- Data simulator
- API server

### Step 3: Access the Dashboard
```
http://localhost:3000
```

### Step 4: Monitor Logs
```bash
# Watch all services
docker-compose logs -f

# Watch specific service
docker-compose logs -f flink-jobmanager
```

## 📊 System Architecture

```
┌─────────────────┐
│ Taxi Simulator  │ (Generates GPS, speed, location data)
└────────┬────────┘
         │
         ▼
    ┌─────────┐
    │ Kafka   │ (Topic: taxi-events)
    └────┬────┘
         │
         ▼
  ┌──────────────┐
  │ Flink Job    │ (Process events, detect anomalies)
  │ - Speeding   │
  │ - Geofence   │
  │ - Analytics  │
  └────┬─────────┘
       │
       ▼
   ┌────────┐
   │ Redis  │ (Store results)
   └────┬───┘
        │
        ▼
  ┌──────────────┐
  │ Flask API    │ (Serve data)
  └────┬─────────┘
       │
       ▼
  ┌────────────┐
  │ Dashboard  │ (React UI)
  └────────────┘
```

## 🔧 Configuration

### Environment Variables
Create a `.env` file in the project root:
```env
KAFKA_BROKER=kafka:9092
REDIS_HOST=redis
REDIS_PORT=6379
FLINK_JM_HOST=flink-jobmanager
FLINK_JM_PORT=8081
API_PORT=5000
DASHBOARD_PORT=3000
```

### Kafka Topics
- `taxi-events` - Raw taxi data
- `speeding-alerts` - Speeding detection
- `geofence-alerts` - Area violation alerts
- `analytics-stream` - Real-time metrics

## 📈 Key Features

### 1. Real-Time Data Processing
- Process thousands of taxi events per second
- Sub-second latency detection

### 2. Event Detection
- **Speeding Detection**: Alert when taxi exceeds speed limit
- **Geofencing**: Monitor taxi movement within defined zones
- **Metrics**: Calculate average speed, distance, duration

### 3. Live Dashboard
- Real-time map visualization
- Event streams and alerts
- Fleet statistics
- Performance metrics

### 4. Scalability
- Horizontally scalable Flink cluster
- Redis for fast data access
- Containerized architecture

## 🧪 Testing

### Test Data Generation
```bash
# Run individual simulator
python data-simulator/taxi_simulator.py
```

### Kafka Topic Testing
```bash
# Check Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Consume messages
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic taxi-events
```

### Redis Verification
```bash
# Connect to Redis
docker exec -it redis redis-cli

# View keys
KEYS *

# Get sample data
GET taxi:123
```

## 📚 Learning Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Flink Documentation](https://flink.apache.org/what-is-flink/flink-architecture/)
- [Redis Documentation](https://redis.io/documentation)
- [Docker Compose Guide](https://docs.docker.com/compose/)

## 🐛 Troubleshooting

### Kafka Connection Issues
```bash
# Check if Kafka is running
docker ps | grep kafka

# Inspect Kafka logs
docker-compose logs kafka
```

### Flink Job Failures
```bash
# Check Flink UI
http://localhost:8081

# View task logs
docker-compose logs flink-taskmanager
```

### Redis Connection Error
```bash
# Verify Redis is running
docker exec redis redis-cli ping

# Should return: PONG
```

## 📝 Development Workflow

1. Create feature branch from `main`
2. Make changes and test locally
3. Submit pull request
4. After review, merge to `main`

## 🤝 Contributing

Contributions are welcome! Please follow these steps:
1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Push to the branch
5. Create a Pull Request

## 📄 License

This project is part of the Institute for Data Engineering - Big Data Lab Exercises.

## ✉️ Support

For issues or questions, please create an issue on GitHub or contact the maintainers.

---

**Last Updated**: May 2026  
**Status**: 🟢 In Development
