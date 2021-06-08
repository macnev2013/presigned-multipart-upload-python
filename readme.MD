# Presigned Multipart Upload with Python

Pull the latest code.
```
docker pull localstack/localstack
```

Use the following commands to run the code.

```
docker-compose up -d
```

Wait for docker to start

```
python main.py
```

Testing if the latest code has reflected.
```
awslocal s3 ls s3://bucket/key
```

Cleanup
```
docker-compose down
```