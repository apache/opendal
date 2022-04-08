# Use azblob as backend

[azblob.rs](azblob.rs) provides a detailed examples for using azblob as backend.

This example has native support for Azure Storage Simulator [Azurite](https://github.com/Azure/Azurite). All value will fall back to Azurite default settings.

We can start a mock services like:

```shell
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite
az storage container create --name test --connection-string "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
```

Then start our azblob examples

```shell
OPENDAL_AZBLOB_CONTAINER=test cargo run --example azblob
```

All config could be passed via environment:

- `OPENDAL_AZBLOB_ROOT`: root path, default: /
- `OPENDAL_AZBLOB_CONTAINER`: container name
- `OPENDAL_AZBLOB_ENDPOINT`: endpoint of your container
- `OPENDAL_AZBLOB_ACCOUNT_NAME`: account name
- `OPENDAL_AZBLOB_ACCOUNT_KEY`: account key
