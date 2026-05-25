import React from 'react';
import './AlertsPanel.css';

const AlertsPanel = ({ alerts }) => {
  const getAlertColor = (eventType) => {
    switch (eventType) {
      case 'SPEEDING':
        return '#ff4444';
      case 'GEOFENCE_VIOLATION':
        return '#ff9900';
      default:
        return '#4444ff';
    }
  };

  const getAlertIcon = (eventType) => {
    switch (eventType) {
      case 'SPEEDING':
        return '🚨';
      case 'GEOFENCE_VIOLATION':
        return '⚠️';
      default:
        return 'ℹ️';
    }
  };

  return (
    <div className="alerts-panel">
      <h2>🔔 Recent Alerts</h2>
      <div className="alerts-container">
        {alerts.length === 0 ? (
          <p className="no-alerts">No alerts</p>
        ) : (
          alerts.map((alert, index) => (
            <div
              key={index}
              className="alert-item"
              style={{ borderLeftColor: getAlertColor(alert.event_type) }}
            >
              <div className="alert-header">
                <span className="alert-icon">{getAlertIcon(alert.event_type)}</span>
                <span className="alert-type">{alert.event_type}</span>
                <span className="alert-taxi">Taxi #{alert.taxi_id}</span>
              </div>
              <div className="alert-details">
                <p>Speed: {alert.speed_kmh.toFixed(2)} km/h</p>
                <p>Position: ({alert.lat.toFixed(4)}, {alert.lon.toFixed(4)})</p>
                <p className="alert-time">
                  {new Date(alert.timestamp).toLocaleTimeString()}
                </p>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
};

export default AlertsPanel;
