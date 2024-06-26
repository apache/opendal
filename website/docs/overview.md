---
sidebar_label: Overview
sidebar_position: 1
---

# Welcome to Apache OpenDAL™

OpenDAL represents **Open** **D**ata **A**ccess **L**ayer. Our vision is to **access data freely**.

## What does OpenDAL do?

![](https://user-images.githubusercontent.com/5351546/222356748-14276998-501b-4d2a-9b09-b8cff3018204.png)

## Getting started

See the page for quick start with multiple languages: [Quickstart](quickstart.md).

## Why OpenDAL?

The vision of OpenDAL is access data freely, where "free" refers to four essential aspects:

### 1. Free from services

OpenDAL must enable users to access various storage services ranging from `s3` to `dropbox` via its own native API. It should provide a unified API for accessing all these services.

For example, we DO

- Add support for [Google Drive](https://www.google.com/drive/): It allows users to access and manage their data on the [Google Drive](https://www.google.com/drive/).
- Add support for [Object Storage Service (OSS)](https://www.alibabacloud.com/product/object-storage-service) via native API: Users can utilize Aliyun's RAM support.
- Add support for [supabase storage](https://supabase.com/docs/guides/storage): Users can visit `supabase storage` now!

while we DO NOT

- Add support for [Google Cloud Storage (GCS)](https://cloud.google.com/storage) via [XML API](https://cloud.google.com/storage/docs/xml-api/overview): [GCS](https://cloud.google.com/storage) has native [JSON API](https://cloud.google.com/storage/docs/json_api) which is more powerful
- Add support for structural data in `MySQL/PostgreSQL`: We can treat a database as a simple key-value store, but we can't support unified access of structural data.

### 2. Free from implementations

OpenDAL needs to separate the various implementation details of services and enables users to write identical logic for different services.

For example, we DO

- Add a new capability to indicate whether `presign` is supported: Users can now write logic based on the `can_presign` option.
- Add a `default_storage_class` configuration for the S3 service: Configuration is specific to the S3 service.
- Add an option for `content_type` in the `write` operation: It aligns with HTTP standards.

while we DO NOT

- Add a new option in read for `storage_class`: As different services could have varying values for this parameter.

### 3. Free to integrate

OpenDAL needs to be integrated with different systems.

For example, we DO

- Add Python binding: Python programmers can use OpenDAL.
- Add object_store integration: `object_store` users can adopt OpenDAL.

### 4. Free of cost

OpenDAL needs to implement features in a zero-cost way which means:

- Users don't need to pay costs for unused features.
- Users cannot write better implementation for used features.

For example, we DO

- Add `layer` support: Users can add logging/metrics/tracing in zero cost way.
- Implement `seek` for Reader: Users cannot write better `seek` support, they all need to pay the same cost.

we DO NOT

- Add `Arc` for metadata: Users may only need to use metadata once and never clone it. For those who do want this feature, they can add `Arc` themselves.
