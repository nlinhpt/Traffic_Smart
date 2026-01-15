# Data Engineer Final Project: Smart Traffic HCMC

## Setup

Clone this repository:

```bash
git clone https://github.com/nauxqouh/Traffic-HCMC-ETL-Analysis-Project.git
```

Ensure `jars/` folder in repo. Download here: [jars download](https://drive.google.com/drive/folders/1lppS2eHXyM_IdzRfN-xD4AlSHx1A-7Fh?usp=sharing)


Build and start container:
```bash
docker compose up -d
```


## Run Spark

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
/opt/spark-apps/bronze/traffic_bronze_ingest.py
```
