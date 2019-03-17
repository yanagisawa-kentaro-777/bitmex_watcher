## Using docker-compose. (Recommended)

```bash
docker-compose up -d
```
```bash
docker-compose ps
```
```bash
docker logs watcher-node
```
```bash
docker exec -it watcher-node /bin/bash
```
```bash
docker-compose stop
```

## Running a MongoDB node.

```bash
$ docker run --name mongo-node -d mongo:4.0.6
```

or if you want to connect to the mongo-node from outside of the docker:

```bash
$ docker run --name mongo-node -d -p 27017:27017 mongo:4.0.6 --bind_ip 0.0.0.0
$ docker-machine ip
```

## Running a Redis node.

```bash
$ docker run --name redis-node -d redis:5.0.3
```

## Building and running a watcher node.

```bash
$ docker build -t watcher .
$ docker run -it --rm --name watcher-node --link mongo-node:mongo --link redis-node:redis watcher
```

or if you want to log in to the watcher node without starting the watcher program:

```bash
$ docker run -it --rm --name watcher-node --link mongo-node:mongo --link redis-node:redis watcher /bin/bash
```
