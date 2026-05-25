"""
Apache Flink Streaming Job for Real-Time Traffic Monitoring
Consumes taxi events from Kafka and processes them in real-time
Detects speeding, geofence violations, and calculates analytics
Stores results in Redis for dashboard consumption
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, FilterFunction
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Redis connection
redis_client = redis.Redis(
    host='redis',
    port=6379,
    db=0,
    decode_responses=True
)


class TaxiEventParser(MapFunction):
    """Parse and enrich taxi events"""

    def map(self, value: str) -> Dict[str, Any]:
        """Parse JSON event"""
        try:
            event = json.loads(value)
            event['processing_timestamp'] = datetime.utcnow().isoformat() + 'Z'
            return event
        except Exception as e:
            logger.error(f"Error parsing event: {e}")
            return None


class EventFilter(FilterFunction):
    """Filter out None values"""

    def filter(self, value: Dict[str, Any]) -> bool:
        """Filter function"""
        return value is not None


class SpeedingDetector(MapFunction):
    """Detect speeding violations"""

    def __init__(self):
        self.speed_limit = 50  # km/h

    def map(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Check for speeding"""
        if event['speed_kmh'] > self.speed_limit:
            event['is_speeding'] = True
            # Store alert in Redis
            alert_key = f"alert:SPEEDING:{event['taxi_id']}_{event['timestamp']}"
            redis_client.setex(alert_key, 3600, json.dumps(event))  # 1 hour expiry

            # Update speeding counter
            redis_client.incr('stats:speeding')

            logger.warning(f"SPEEDING: Taxi {event['taxi_id']} at {event['speed_kmh']} km/h")
        else:
            event['is_speeding'] = False

        return event


class GeofenceDetector(MapFunction):
    """Detect geofence violations"""

    def __init__(self):
        self.center_lat = 40.7128
        self.center_lon = -74.0060
        self.radius_km = 10

    def map(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Check geofence"""
        lat_diff = event['lat'] - self.center_lat
        lon_diff = event['lon'] - self.center_lon
        distance_km = (lat_diff ** 2 + lon_diff ** 2) ** 0.5 * 111

        if distance_km > self.radius_km:
            event['in_geofence'] = False
            # Store alert in Redis
            alert_key = f"alert:GEOFENCE_VIOLATION:{event['taxi_id']}_{event['timestamp']}"
            redis_client.setex(alert_key, 3600, json.dumps(event))

            # Update geofence violation counter
            redis_client.incr('stats:geofence_violation')

            logger.warning(f"GEOFENCE: Taxi {event['taxi_id']} at {distance_km:.2f} km from center")
        else:
            event['in_geofence'] = True

        return event


class AnalyticsProcessor(MapFunction):
    """Process analytics and update statistics"""

    def map(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Update analytics"""
        taxi_id = event['taxi_id']

        # Store current taxi position
        taxi_key = f"taxi:{taxi_id}:current"
        redis_client.setex(taxi_key, 60, json.dumps(event))  # 60 seconds expiry

        # Update statistics
        redis_client.incr('stats:total_taxis')
        redis_client.incr('stats:active_taxis')

        # Update speed statistics
        current_avg = float(redis_client.get('stats:avg_speed') or 0)
        current_count = int(redis_client.get('stats:speed_update_count') or 1)
        new_avg = (current_avg * current_count + event['speed_kmh']) / (current_count + 1)
        redis_client.set('stats:avg_speed', new_avg)
        redis_client.incr('stats:speed_update_count')

        # Update max speed
        current_max = float(redis_client.get('stats:max_speed') or 0)
        if event['speed_kmh'] > current_max:
            redis_client.set('stats:max_speed', event['speed_kmh'])

        # Update alert counter
        if event.get('is_speeding') or not event.get('in_geofence'):
            redis_client.incr('stats:total_alerts')

        return event


def create_kafka_consumer() -> FlinkKafkaConsumer:
    """Create Kafka consumer"""
    return FlinkKafkaConsumer(
        'taxi-events',
        SimpleStringSchema(),
        {'bootstrap.servers': 'kafka:9092', 'group.id': 'flink-group'}
    )


def create_kafka_producer() -> FlinkKafkaProducer:
    """Create Kafka producer for processed events"""
    return FlinkKafkaProducer(
        'processed-taxi-events',
        SimpleStringSchema(),
        {'bootstrap.servers': 'kafka:9092'}
    )


def main():
    """Main Flink job"""
    logger.info("Starting Flink streaming job")

    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Create Kafka source
    kafka_consumer = create_kafka_consumer()
    taxi_events = env.add_source(kafka_consumer)

    # Process pipeline
    processed_events = (
        taxi_events
        .map(TaxiEventParser())  # Parse events
        .filter(EventFilter())  # Filter None values
        .map(SpeedingDetector())  # Detect speeding
        .map(GeofenceDetector())  # Detect geofence violations
        .map(AnalyticsProcessor())  # Update analytics
    )

    # Add sink to Kafka
    kafka_producer = create_kafka_producer()
    processed_events.add_sink(
        kafka_producer.sink_function(
            lambda event: json.dumps(event).encode('utf-8')
        )
    )

    # Execute job
    env.execute("Taxi Monitoring Pipeline")


if __name__ == '__main__':
    main()
