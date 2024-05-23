File is at:

> https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-05-01_anthem_index.json.gz

Can we avoid downloading the whole thing?

> curl -I https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-05-01_anthem_index.json.gz

```
HTTP/1.1 200 OK
x-amz-id-2: +2MGiWUl7Sdor32dKoGp9f6N6D5xn8FTN9UqdesJB/S4kM8igX36mDcxSP74kvkLI7xilmxNeJc=
x-amz-request-id: KBQZF9RRF1FTNH2H
Date: Wed, 22 May 2024 01:28:50 GMT
x-amz-replication-status: COMPLETED
Last-Modified: Tue, 30 Apr 2024 23:35:14 GMT
x-amz-expiration: expiry-date="Thu, 01 May 2025 00:00:00 GMT", rule-id="Default"
ETag: "c06992183feb219256194ca5f3d65ad3-1334"
x-amz-server-side-encryption: AES256
x-amz-version-id: yidnEuhfg4dokhmxE6BAsVQJQBj0IdXQ
Accept-Ranges: bytes
Content-Type: binary/octet-stream
Server: AmazonS3
Content-Length: 11184659576
```

We have Accept-Ranges: bytes, so yes, we can get a range of bytes from the file.


Setup:

```bash
ipython kernel install --user --name=takehome-kernel
```
