Scheduled web content producer for Azure Event Hubs using Kafka protocol. Supports FlareSolverr for Cloudflare bypass.

```
# Pull image
docker pull ghcr.io/jbouwens/cronwebarchivertokafka:master

# Run container
docker run -d \
  --name cronwebarchivertokafka \
  -v /path/to/appsettings.json:/app/appsettings.json:rw \
  -e TZ=America/New_York \
  ghcr.io/jbouwens/cronwebarchivertokafka:master
```
