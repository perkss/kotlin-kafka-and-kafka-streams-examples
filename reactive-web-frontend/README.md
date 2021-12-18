```shell script
docker build --tag reactive-web-ui:latest .
```

Run locally

```shell script
docker run -p 8000:80 reactive-web-ui
```

```shell script
docker tag reactive-web-ui $(minishift openshift registry)/myproject/reactive-web-ui
docker push $(minishift openshift registry)/myproject/reactive-web-ui
```

```shell script
oc new-app --image-stream=reactive-web-ui --name=reactive-web-ui
oc expose service reactive-web-ui
```