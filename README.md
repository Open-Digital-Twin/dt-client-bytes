# dt-client

### MQTT QoS
Environment variable `MQTT_CLIENT_QOS` must be defined.

```
pub enum QoS {
  AtMostOnce = 0,
  AtLeastOnce = 1,
  ExactlyOnce = 2
}
```
