Dataset: https://huggingface.co/datasets/maharshipandya/spotify-tracks-dataset

```bash
python ./create_dataset.py
docker-compose up
./run_w.ps1
```

|                   | 1 DataNode       | 3 DataNode       |
|-------------------|------------------|------------------|
| no optimization   |                  |                  |
| with optimization |                  |                  |

Spark Applicaton UI (jobs, stages....):  http://localhost:4040

Spark Master UI: http://localhost:8080

HDFS: http://localhost:9870
