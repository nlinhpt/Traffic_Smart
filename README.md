# Data Engineer Final Project: Smart Traffic HCMC

## Folder Structure

```bash
Traffic-HCMC-ETL/
├── docker-compose.yml       # File điều khiển toàn bộ hệ thống
├── Dockerfile               # Để build image Spark có sẵn thư viện minio
├── .gitignore               
├── jars/                    # Nơi chứa các file .jar
│   ├── hadoop-aws-3.3.4.jar
│   ├── aws-java-sdk-bundle-1.12.262.jar
│   └── postgresql-42.7.8.jar
├── spark-data/               
│   └── input/               # Chứa 2 file JSON 4GB 
├── spark-apps/              # Nơi chứa toàn bộ logic xử lý (Gắn vào /opt/spark-apps)
│   ├── bronze/              # Các script nạp data thô
│   │   └── traffic_bronze_ingest.py
│   ├── silver/              # Các script làm sạch, ép kiểu
│   │   └── ..
│   └── gold/                # Các script tổng hợp, đẩy vào Postgres
│       └── ..
├── conf/  
│   └── spark-defaults.conf  # Cấu hình Spark (nếu cần)
└── README.md                # Hướng dẫn chạy đồ án
```

## Setup

**Step 1:** Clone this repository:

```bash
git clone https://github.com/nauxqouh/Traffic-HCMC-ETL-Analysis-Project.git
```

**Step 2:** Ensure that `spark-data/input` and `jars/` folder structure as above. 

Download here: 
- [jars download](https://drive.google.com/drive/folders/1lppS2eHXyM_IdzRfN-xD4AlSHx1A-7Fh?usp=sharing)
- [Traffic Data](https://www.kaggle.com/datasets/ren294/iot-car-hcmcity)


**Step 3:** Build and start container:
```bash
docker compose up -d
```

## Run Spark

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
/opt/spark-apps/bronze/traffic_bronze_ingest.py
```