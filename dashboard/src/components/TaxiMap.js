import React from 'react';
import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet';
import L from 'leaflet';
import './TaxiMap.css';

// Fix for default marker icon
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: require('leaflet/dist/images/marker-icon-2x.png'),
  iconUrl: require('leaflet/dist/images/marker-icon.png'),
  shadowUrl: require('leaflet/dist/images/marker-shadow.png'),
});

const TaxiMap = ({ taxis }) => {
  const center = [40.7128, -74.0060]; // NYC

  const getTaxiIcon = (taxi) => {
    let color = 'blue';
    if (taxi.is_speeding) {
      color = 'red';
    } else if (!taxi.in_geofence) {
      color = 'orange';
    }

    return new L.Icon({
      iconUrl: `https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-${color}.png`,
      shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
      iconSize: [25, 41],
      iconAnchor: [12, 41],
      popupAnchor: [1, -34],
      shadowSize: [41, 41]
    });
  };

  return (
    <div className="taxi-map">
      <h2>Live Fleet Map</h2>
      <MapContainer center={center} zoom={12} scrollWheelZoom={false} style={{ height: '100%', width: '100%' }}>
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {taxis.map((taxi) => (
          <Marker key={taxi.taxi_id} position={[taxi.lat, taxi.lon]} icon={getTaxiIcon(taxi)}>
            <Popup>
              <div className="popup-content">
                <strong>Taxi #{taxi.taxi_id}</strong>
                <p>Speed: {taxi.speed_kmh.toFixed(2)} km/h</p>
                <p>Passengers: {taxi.passengers}</p>
                <p>Status: {taxi.status}</p>
                {taxi.is_speeding && <p style={{ color: 'red' }}>⚠️ SPEEDING</p>}
                {!taxi.in_geofence && <p style={{ color: 'orange' }}>⚠️ Out of Geofence</p>}
              </div>
            </Popup>
          </Marker>
        ))}
      </MapContainer>
    </div>
  );
};

export default TaxiMap;
