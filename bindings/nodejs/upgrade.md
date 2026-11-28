# Upgrade to v0.50

## Breaking change

### Structural `Metadata` and `Capability` implementations

`Metadata` and `Capability` are now generated from OpenDAL core and gain
members that the hand-written classes lacked. Code that only uses the
instances returned by `Operator`, such as `await op.stat(path)` and
`op.capability()`, needs no change.

TypeScript classes and test doubles that implement these classes structurally,
such as `class CachedMetadata implements Metadata`, fail to compile (`TS2720`)
until they declare the new members. The added members also compile against
v0.49.13, so you can add them before upgrading.

`Metadata` gains `contentLanguage` and `isCurrent()`. Both can return `null`
when the value is unknown:

```diff
 import type { Metadata } from 'opendal'

 class CachedMetadata implements Metadata {
   // ... existing members ...
+  get contentLanguage(): string | null {
+    return null
+  }
+  isCurrent(): boolean | null {
+    return null
+  }
 }
```

`Capability` gains getters for the core capabilities it did not expose before,
such as `restore`, `deleteWithRecursive`, and `writeWithChecksum`. Declare each
one with the type from `generated.d.ts`: capability flags are `boolean`, so
return `false` for an unsupported operation, and the size limits
`deleteMaxSize`, `copyMultiMaxSize`, and `copyMultiMinSize` are
`bigint | null`.

# Upgrade to v0.48

## Breaking change

### Public API

Now, nodejs binding `op.is_exist` changed to `op.exists` to align with nodejs API style.

# Upgrade to v0.47

## Breaking change

### Public API

Now, the `append` operation has been removed. You can use below code instead.

```js
op.write("path/to/file", Buffer.from("hello world"), { append: true });
```

# Upgrade to v0.44

## Breaking change

### Services

Because of [a TLS lib issue](https://github.com/apache/opendal/issues/3650), we temporarily disable the `services-ftp` feature.

### Public API

Now, the `list` operation returns `Array<Entry>` instead of a lister.
Also, we removed `scan`, you can use `list('some/path', {recursive: true})`/`listSync('some/path', {recursive: true})` instead of `scan('some/path')`/`scanSync('some/path')`.
