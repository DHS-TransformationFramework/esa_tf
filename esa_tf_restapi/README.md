# ESA transformation framework REST API

## How to run

### Development

```bash
make start
```

### Run service

```bash
make serve
```

Remember to define the `WEB_CONCURRENCY` envvar (default is 1).

## Running tests

```bash
make test
```

## Endpoints

### Common Schema Definition Language (CSDL)

```bash
curl "http://localhost:8000/\$metadata"
```

### List of plugins

```bash
curl http://localhost:8000/Workflows | jq
```

### Access a single plugin definition

```bash
curl "http://localhost:8000/Workflows('6c18b57d-fgk4-1236-b539-12h305c26z89')" | jq
```

### List of transformation orders

```bash
curl http://localhost:8000/TransformationOrders | jq
```

It is also possible to filter accessible orders:

```bash
curl "http://localhost:8000/TransformationOrders?\$filter=`jq -rn --arg x "Status eq 'completed'" '$x|@uri'`" | jq
```

### Request a new transformation

```bash
curl -d '{"key1":"value1", "key2":"value2"}' -H "Content-Type: application/json" http://localhost:8000/TransformationOrders | jq
```

### State of a transformation

```bash
curl "http://localhost:8000/TransformationOrders('2b17b57d-fff4-4645-b539-91f305c26x53')" | jq
```

