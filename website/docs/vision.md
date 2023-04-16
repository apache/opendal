---
title: Vision
sidebar_position: 2
---

OpenDAL VISION is: **access data freely**.

---

This is an overview of what the shape of OpenDAL looks like, but also somewhat zoomed out, so that the vision can survive while the exact minute details might shift and change over time.

## 1. Free from services

OpenDAL must enable users to access various storage services ranging from `s3` to `dropbox` via it's own native API. It should provide a unified API for accessing all these services.

### Examples

- Add support for [Google Drive](https://www.google.com/drive/): Good, it allows users to access and manage their data on the [Google Drive](https://www.google.com/drive/).
- Add support for oss via native API: Good, users can utilize Aliyun's RAM support.
- Add support for [supabase storage](https://supabase.com/docs/guides/storage): Good, users can visit supabase storage now!


- Add support for [GCS](https://cloud.google.com/storage) via [XML API](https://cloud.google.com/storage/docs/xml-api/overview): Bad, [GCS](https://cloud.google.com/storage) has native [JSON API](https://cloud.google.com/storage/docs/json_api) which more powerful
- Add support for MySQL/PostgreSQL: Bad, relational DBMS provides data types such as BLOB, but they are often not used as a storage service.

## 2. Free from implementations

OpenDAL needs to separate the various implementation details of services and enables users to write identical logic for different services.

### Examples

- Add a new capability to indicate whether or not `presign` is supported: Good, users can now write logic based on the `can_presign` option.
- Add a `default_storage_class` configuration for the S3 service: Good, configuration is specific to the s3 service.
- Add an option for `content_type` in the `write` operation: Good, it aligns with HTTP standards.


- Add a new option in read for `storage_class`: Bad, as different services could have varying values for this parameter.

## 3. Free to integrate

OpenDAL needs to be integrated with different systems.

### Examples

- Add python binding: Good, users from `python` can use OpenDAL.
- Add object_store integration: Good, users of `object_store` can adopt OpenDAL.

## 4. Free to zero cost

OpenDAL needs to implement features in zero cost way which means:

- Users don't need to pay cost for not used features.
- Users can't write better implementation for used features.

### Examples

- Add `layer` support: Good, users can add logging/metrics/tracing in zero cost way.
- Implement `seek` for Reader: Good, users can't write better `seek` support, they all need to pay the same cost.


- Add `Arc` for metadata: Bad, users may only need to use metadata once and never clone it. For those who do want this feature, they can add `Arc` themselves
