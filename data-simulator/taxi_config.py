"""
Taxi Configuration - Defines simulation parameters
"""

import os

# City Configuration (New York City as example)
CITY_CENTER_LAT = float(os.getenv('CITY_CENTER_LAT', 40.7128))
CITY_CENTER_LON = float(os.getenv('CITY_CENTER_LON', -74.0060))
CITY_RADIUS_KM = float(os.getenv('CITY_RADIUS_KM', 15))

# Taxi Configuration
NUM_TAXIS = int(os.getenv('NUM_TAXIS', 100))
UPDATE_FREQUENCY = int(os.getenv('UPDATE_FREQUENCY', 5))  # seconds

# Speed Configuration
MIN_SPEED_KMH = 0
MAX_SPEED_KMH = 80
SPEED_LIMIT_KMH = float(os.getenv('SPEED_LIMIT_KMH', 50))

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'taxi-events')

# Geofence Configuration
GEOFENCE_RADIUS_KM = 10  # Alert if taxi leaves this radius

# Event Types
EVENT_SPEEDING = 'SPEEDING'
EVENT_GEOFENCE = 'GEOFENCE_VIOLATION'
EVENT_NORMAL = 'NORMAL'
