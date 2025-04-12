Dataset: https://huggingface.co/datasets/maharshipandya/spotify-tracks-dataset

Run:
```
pip install -r requirements.txt
python ./create_dataset.py
docker-compose up
./run_w.ps1
```

|                   | 1 DataNode       | 3 DataNode       |
|-------------------|------------------|------------------|
| no optimization   | 44.99s 44.11 MB  | 46.49s  43.90 MB |
| with optimization | 49.07s 44.13 MB  | 45.90s  44.02 MB |

Spark Applicaton UI (jobs, stages....):  http://localhost:4040
Spark Master UI: http://localhost:8080
HDFS: http://localhost:9870