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


### How to cross-compile

https://github.com/docker/buildx
https://cloudolife.com/2022/03/05/Infrastructure-as-Code-IaC/Container/Docker/Docker-buildx-support-multiple-architectures-images/

```
docker run --privileged --rm tonistiigi/binfmt --install all
docker buildx create --use --name mybuild node-amd64
docker buildx create --append --name mybuild node-arm64
docker buildx build --platform linux/amd64,linux/arm64 . -t opendigitaltwin/dt-client-bytes:latest --push
```
