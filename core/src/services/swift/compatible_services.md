## Compatible Services

### OpenStack Swift

[OpenStack Swift](https://docs.openstack.org/swift/latest/) is the default implementations of swift servcies.

Opendal supports two methods for connecting to OpenStack Swift:

In the first method, we need to set:

- `endpoint`: The endpoint of OpenStack Swift, for example: `http://127.0.0.1:8080`.
- `account_name`: The name of OpenStack Swift account.
- `container`: The name of OpenStack Swift container.
- `token`: OpenStack Swift container personal access token.

```rust,ignore
builder.endpoint("http://127.0.0.1:8080");
builder.account_name("account");
builder.container("container");
builder.token("token");
```

In the second method, we need to set:

- `storage_url`: The url of OpenStack Swift account resource, for example: `http://127.0.0.1:8080/v1/account`.
- `container`: The name of OpenStack Swift container.
- `token`: OpenStack Swift container personal access token.

```rust,ignore
builder.storage_url("http://127.0.0.1:8080/v1/account/");
builder.container("container");
builder.token("token");
```

`storage_url` is the full URL of OpenStack Swift account resource used to replace the endpoint and account configurations. Both `storage_url` and `token` can be obtained through OpenStack Swift authentication service.

For more information, refer:

* [OpenStack Swift API](https://docs.openstack.org/api-ref/object-store/).
* [OpenStack Swift Authentication](https://docs.openstack.org/swift/latest/api/object_api_v1_overview.html).

### Ceph Rados Gateway

[Ceph Rados Gateway](https://docs.ceph.com/en/quincy/radosgw/) supports a RESTful API that is compatible with the basic data access model of OpenStack Swift API.

To connect to Ceph Rados Gateway, we need to set:

- `storage_url`: The url of swift account resource, for example: `http://127.0.0.1:8080/swift/v1`.
- `container`: The name of swift container.
- `token`: swift container personal access token.

```rust,ignore
builder.storage_url("http://127.0.0.1:8080/swift/v1");
builder.container("container");
builder.token("token");
```

For more information, refer:

* [Ceph Rados Gateway Swift API](https://docs.ceph.com/en/latest/radosgw/swift/#api).
* [Ceph Rados Gateway Swift Authentication](https://docs.ceph.com/en/latest/radosgw/swift/auth/).
