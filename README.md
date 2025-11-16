### Standalone Implementation Test

First build a docker image:
```bash
cd code/standalone
docker build -t standalone-kv -f standalone.Dockerfile .
```
Then run a container of the image:
```bash
docker run --rm -p 20160:20160 standalone-kv --addr=0.0.0.0:20160 --path=/data
```
A simple test case:
```bash
brew install grpcurl # install grpcurl if needed

grpcurl -plaintext \
  -proto ./code/standalone/proto/proto/rawkv.proto \
  -d '{"key":"YQ==","value":"MQ=="}' \
  localhost:20160 rawkv.RawKV/RawPut

grpcurl -plaintext \
  -proto ./code/standalone/proto/proto/rawkv.proto \
  -d '{"key":"YQ=="}' \
  localhost:20160 rawkv.RawKV/RawGet
```