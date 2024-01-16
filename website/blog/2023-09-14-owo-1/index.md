---
title: "OwO #1: The v0.40 Release"
date: 2023-09-15
slug: owo-1
tags: [owo]
authors:
  - name: Xuanwo
    url: https://github.com/Xuanwo
    image_url: https://github.com/Xuanwo.png
---

> OwO (Outcome, Working, Outlook) is an Apache OpenDAL™ release blog series, where we share the current work status and future plans.

Hello! It's been a while since our last update. We've been hard at work determining the optimal way to implement new features and improvements. We're thrilled to announce that we'll soon be releasing v0.40.

This post is structured into three main sections:

- Outcome (1st `O` in `OwO`): Summarizes the key accomplishments in the v0.40 release.
- Working (the `w` in `OwO`): Provides an update on our current work.
- Outlook (2nd `O` in `OwO`): Discusses what lies ahead for OpenDAL.

## Outcome

OpenDAL now comprises four primary components:

- Core: The core library written in Rust.
- Bindings: Language bindings powered by the OpenDAL Rust core.
- Applications: Applications built using the OpenDAL Rust core.
- Integrations: Collaborations with other projects.

### Core

#### Unifying Append and Write Functions

OpenDAL has supported `append` operations since `v0.36`. We've found, however, that this led to significant duplication between append and write. As a result, we've streamlined the two functionalities into a single write function. Our users can now:

```rust
let mut w = op.writer_with("test.txt").append(true).await?;
w.write(content_a).await?;
w.write(content_b).await?;
w.close().await?;
```

This way, users can reuse the `Writer` in their own logic without handling `append` separately.

#### New Lister API

To improve API consistency, we've made some adjustments to our listing functions. We've added `list` and `list_with` methods that perform single operations and renamed the original `list` to `lister` and `lister_with`.

```rust
// Old API
let lister: Lister = op.list("dir").await?;

// New API
let entries: Vec<Entry> = op.list("dir").await?;
let lister: Lister = op.lister("dir").await?;
```

This brings uniformity to our API offerings.

#### List With Metakey

To speed up list operations, OpenDAL can now fetch and store metadata during the listing process. This eliminates the need for separate metadata calls:

```rust
let entries: Vec<Entry> = op
  .list_with("dir/")
  .metakey(Metakey::ContentLength | Metakey::ContentType).await?;

// Use the metadata directly!
let meta: &Metadata = entries[0].metadata();
```

This makes metadata retrieval more intuitive.

#### Buffered Writer

We've added general buffer support to optimize writing operations.

```rust
let w = op.writer_with("path/to/file").buffer(8 * 1024 * 1024).await?
```

#### Others

Other improvements in the core library can be found in our [CHANGELOG](https://github.com/apache/incubator-opendal/blob/main/CHANGELOG.md).

### Bindings

#### C++

[`opendal-cpp`](https://github.com/apache/incubator-opendal/tree/main/bindings/cpp) is ready for its first release! Welcome to check it out and give us some feedback.

#### Haskell

[`opendal-hs`](https://github.com/apache/incubator-opendal/tree/main/bindings/haskell) is ready for its first release! Welcome to check it out and give us some feedback.

#### Java

[`opendal-java`](https://github.com/apache/incubator-opendal/tree/main/bindings/java) enabled more available services in this release, allowing user to visit services like `redis` that not enabled by default in rust core. And `opendal-java` enabled blocking layer to allow users visit services like `s3` in blocking way.

Welcome to integrate `opendal-java` into your project and give us some feedback.

#### New bindings!

- [`opendal-dotnet`](https://github.com/apache/incubator-opendal/tree/main/bindings/dotnet)
- [`opendal-php`](https://github.com/apache/incubator-opendal/tree/main/bindings/php)

### Applications

#### oay

[oay](https://github.com/apache/incubator-opendal/tree/main/bin/oay) is OpenDAL Gateway that allows users to access OpenDAL services via existing protocols like `s3` and `webdav`. It works like a proxy that forwarding requests to OpenDAL services.

In this release, we implement basic `webdav` support. Users can convert any storage services to a webdav server!

#### oli

[oli](https://github.com/apache/incubator-opendal/tree/main/bin/oay) is OpenDAL CLI that allows users to access storage services via CLI like `s3cmd` and `gcloud` does.

We fixed some experience issues in this release and improved some docs. Welcome to try it out and give us some feedback.

### Integrations

#### object_store

[object_store](https://github.com/apache/incubator-opendal/tree/main/integrations/object_store) instead to implement [`object_store`](https://github.com/apache/arrow-rs/tree/master/object_store)'s trait over OpenDAL Operator so that users can use OpenDAL as a backend for `object_store`.

`object_store` is mostly functional, but there are some edge use cases that OpenDAL has yet to support.

So far, this release hasn't seen progress in this area; we are awaiting the resolution of the issue [Allow list paths that do not end with `/`](https://github.com/apache/incubator-opendal/issues/2762).

## Working

We are working on the following things:

- `object_store` support: Make `object_store` integration works and find a user for it.
- Remove the `/` limitation for path, so we can list a path without ending with `/`.
- Expand the `start-after` support to more services (Address [#2786](https://github.com/apache/incubator-opendal/issues/2786)).

## Outlook

We are exploring some innovative ideas:

- [OpenDAL REST/gRPC API](https://github.com/apache/incubator-opendal/discussions/2951): A REST/gRPC Server for OpenDAL.
- [OpenDAL Cache](https://github.com/apache/incubator-opendal/discussions/2953): OpenDAL native cache libs that allowing users to access data more efficiently.
- [OpenDAL File System](https://github.com/apache/incubator-opendal/discussions/2952): A read-only file system that built upon OpenDAL in rust!
- [kio-opendal](https://github.com/apache/incubator-opendal/discussions/3042): A kio plugin powered by OpenDAL that allows users to visit different storage services in [KDE Dolphin](https://apps.kde.org/dolphin/).
- gvfs-opendal: A gvfs plugin powered by OpenDAL that allows users to visit different storage services in [GNOME Files](https://wiki.gnome.org/Apps/Files)

Feel free to join in the discussion!

## Summary

This marks our first OpenDAL `OwO` post. We welcome your feedback.
