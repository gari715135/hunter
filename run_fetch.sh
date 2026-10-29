#!/bin/bash
docker run -d \
  --network host \
  -v /opt/hunt/config.yaml:/app/config.yaml:ro \
  -v /opt/hunt/logs:/app/logs \
  hunt-data-fetch:1.0.0