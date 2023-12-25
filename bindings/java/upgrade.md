# Upgrade to v0.43

## Breaking change

Because of [a TLS lib issue](https://github.com/apache/incubator-opendal/issues/3650), we temporarily disable the `services-ftp` feature.


# Upgrade to v0.41

## Breaking change for constructing operators

[PR-3166](https://github.com/apache/incubator-opendal/pull/3166) changes the API for constructing operators:

Previous:

```java
new BlockingOperator(scheme, config);
new Operator(scheme, config);
```

Current:

```java
BlockingOperator.of(scheme, config);
Operator.of(scheme, config);
```

Now, there is no public constructor for operators, but only factory methods. In this way, the APIs are free to do arbitrary verifications and preparations before constructing operators.
