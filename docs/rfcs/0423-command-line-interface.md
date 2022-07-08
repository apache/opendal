- Proposal Name: `command_line_interface`
- Start Date: 2022-07-08
- RFC PR: [datafuselabs/opendal#423](https://github.com/datafuselabs/opendal/pull/423)
- Tracking Issue: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/issues/0000)

# Summary

Add command line interface for OpenDAL.

# Motivation

> **Q**: There are so many cli out there, why we still need a cli for OpenDAL?
> 
> **A**: Because there are so many cli out there.

To manipulate our date store in different could service, we need to install different clis:

- [`aws-cli`]/[`s3cmd`]/... for AWS (S3)
- [`azcopy`] for Azure Storage Service
- [`gcloud`] for Google Cloud

Those clis provide native and seamless experiences for their own products but also lock us and our data. 

However, for 80% cases, we just want to do simple jobs like `cp`, `mv` and `rm`. It's boring to figure out how to use them:

- `aws --endpoint-url http://127.0.0.1:9900/ s3 cp data s3://testbucket/data  --recursive`
- `azcopy copy 'C:\myDirectory' 'https://mystorageaccount.blob.core.windows.net/mycontainer' --recursive`
- `gsutil cp data gs://testbucket/`

Can we use them in the same way? Can we let the data flow freely? 

Let's look back OpenDAL's slogan:

**Open Data Access Layer that connect the whole world together**

This is a natural extension for OpenDAL: providing a command line interface!

# Guide-level explanation



# Reference-level explanation

TBD

# Drawbacks

Why should we *not* do this?

# Rationale and alternatives

TBD

# Prior art

TBD

# Unresolved questions

TBD

# Future possibilities

TBD

[`aws-cli`]: https://github.com/aws/aws-cli
[`s3cmd`]: https://s3tools.org/s3cmd 
[`azcopy`]: https://github.com/Azure/azure-storage-azcopy
[`gcloud`]: https://cloud.google.com/sdk/docs/install
