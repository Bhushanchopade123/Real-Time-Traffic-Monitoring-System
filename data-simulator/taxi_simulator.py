"""
Taxi Data Simulator - Generates real-time taxi movement data
Publishes to Kafka topic: taxi-events
"""

import json
import logging
import random
import time
from datetime import datetime
from typing import Dict, Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from taxi_config import (
    CITY_CENTER_LAT, CITY_CENTER_LON, CITY_RADIUS_KM,
    NUM_TAXIS, UPDATE_FREQUENCY, SPEED_LIMIT_KMH,
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, GEOFENCE_RADIUS_KM,
    EVENT_SPEEDING, EVENT_GEOFENCE, EVENT_NORMAL
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TaxiSimulator:
    """Simulates taxi movement and generates events"""

    def __init__(self):
        """Initialize taxi simulator"""
        self.producer = self._init_kafka_producer()
        self.taxis = self._generate_initial_taxis()
        self.event_count = 0

    def _init_kafka_producer(self) -> KafkaProducer:
        """Initialize Kafka producer"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info(f"Connected to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def _generate_initial_taxis(self) -> Dict[int, Dict[str, Any]]:
        """Generate initial taxi positions"""
        taxis = {}
        for taxi_id in range(1, NUM_TAXIS + 1):
            taxis[taxi_id] = {
                'taxi_id': taxi_id,
                'lat': CITY_CENTER_LAT + random.uniform(-0.1, 0.1),
                'lon': CITY_CENTER_LON + random.uniform(-0.1, 0.1),
                'speed_kmh': random.uniform(0, 30),
                'heading': random.uniform(0, 360),
                'status': 'active',
                'passengers': random.randint(0, 4),
            }
        logger.info(f"Generated {NUM_TAXIS} initial taxi positions")
        return taxis

    def _update_taxi_position(self, taxi: Dict[str, Any]) -> Dict[str, Any]:
        """Update taxi position with random movement"""
        # Random walk movement
        lat_change = random.uniform(-0.01, 0.01)
        lon_change = random.uniform(-0.01, 0.01)

        taxi['lat'] += lat_change
        taxi['lon'] += lon_change
        taxi['speed_kmh'] = random.uniform(0, 80)
        taxi['heading'] = random.uniform(0, 360)

        return taxi

    def _check_events(self, taxi: Dict[str, Any]) -> str:
        """Check if taxi triggered any events"""
        # Check speeding
        if taxi['speed_kmh'] > SPEED_LIMIT_KMH:
            return EVENT_SPEEDING

        # Check geofence violation
        lat_diff = taxi['lat'] - CITY_CENTER_LAT
        lon_diff = taxi['lon'] - CITY_CENTER_LON
        distance_km = (lat_diff ** 2 + lon_diff ** 2) ** 0.5 * 111  # Approximate conversion

        if distance_km > GEOFENCE_RADIUS_KM:
            return EVENT_GEOFENCE

        return EVENT_NORMAL

    def _publish_event(self, taxi: Dict[str, Any], event_type: str) -> None:
        """Publish taxi event to Kafka"""
        event = {
            'taxi_id': taxi['taxi_id'],
            'lat': round(taxi['lat'], 6),
            'lon': round(taxi['lon'], 6),
            'speed_kmh': round(taxi['speed_kmh'], 2),
            'heading': round(taxi['heading'], 2),
            'status': taxi['status'],
            'passengers': taxi['passengers'],
            'event_type': event_type,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'event_id': f"{taxi['taxi_id']}_{self.event_count}"
        }

        try:
            future = self.producer.send(KAFKA_TOPIC, value=event)
            future.get(timeout=5)
            self.event_count += 1

            if event_type != EVENT_NORMAL:
                logger.info(f"Event published: Taxi {taxi['taxi_id']} - {event_type}")

        except KafkaError as e:
            logger.error(f"Error publishing event: {e}")

    def run(self) -> None:
        """Main simulation loop"""
        logger.info(f"Starting taxi simulator with {NUM_TAXIS} taxis")
        logger.info(f"Update frequency: {UPDATE_FREQUENCY}s")

        try:
            while True:
                # Update each taxi
                for taxi_id, taxi in self.taxis.items():
                    # Update position
                    self.taxis[taxi_id] = self._update_taxi_position(taxi)

                    # Check events
                    event_type = self._check_events(self.taxis[taxi_id])

                    # Publish event
                    self._publish_event(self.taxis[taxi_id], event_type)

                # Log statistics every 10 events
                if self.event_count % (NUM_TAXIS * 10) == 0:
                    logger.info(f"Published {self.event_count} events total")

                # Sleep before next update
                time.sleep(UPDATE_FREQUENCY)

        except KeyboardInterrupt:
            logger.info("Simulator stopped by user")
        except Exception as e:
            logger.error(f"Error in simulation loop: {e}", exc_info=True)
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")


def main():
    """Main entry point"""
    simulator = TaxiSimulator()
    simulator.run()


if __name__ == '__main__':
    main()
