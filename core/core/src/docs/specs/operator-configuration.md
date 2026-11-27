# Operator configuration

Registry-based operator creation accepts a storage scheme or URI and a flat
set of string options. This specification applies to entry points that forward
configuration to the operator registry. Each build and binding determines
which entry points it exposes and which services, context providers, and layers
it registers.

## Keys and precedence

OpenDAL lowercases ASCII letters in option keys. It decodes URI query keys and
values before normalizing keys or interpreting values. Extra options supplied
alongside a URI are already strings and are used as supplied, apart from key
normalization.

For each normalized key, the last query occurrence wins. Extra options then
override matching query keys, including when the overriding value is empty.
If the entry point supplies an ordered sequence of extra options, the last
entry for a normalized key wins there as well. Supply only one spelling of a
key in an unordered map to get a predictable result.

| Key | Meaning |
| --- | --- |
| `context.<name>.<field>` | Sets an option for a named provider of base runtime resources. |
| `layers` | Lists the layers to add to the operator. |
| `layer.<name>.<field>` | Sets an option for a layer named in `layers`. |
| All other keys | Go to the service selected by the scheme. |

A scoped key requires a name made of ASCII letters, digits, `_`, or `-` and a
nonempty field suffix. The component receives the entire normalized suffix,
including any dots. Malformed scoped keys fail with `ConfigInvalid`, even for
inactive layers. The service receives the remaining options and the URI
components, and decides how to resolve conflicts between them. For example,
S3 uses the URI's bucket and root path when present, even if extra options set
`bucket` and `root`.

## Context and layer order

OpenDAL applies configured context providers in ascending order of their
normalized names to build the base context before applying layers. Omitted
context scopes keep the incoming resources. A provider that recognizes none
of its supplied fields preserves its input context.

OpenDAL splits `layers` at commas, trims ASCII whitespace from each name, and
lowercases ASCII letters. A missing, empty, or ASCII-whitespace-only list adds
zero configured layers. Every name in a nonempty list follows the same rules
as scoped names. Empty entries, trailing commas, and duplicate normalized
names fail with `ConfigInvalid`. A comma encoded as `%2C` in a URI query acts
as a separator after decoding.

Layers are listed from innermost to outermost. With `layers=timeout,retry`,
retry wraps timeout, and both wrap the core defaults. A listed layer with no
scoped fields receives an empty options set. Well-formed options for an unlisted
layer are ignored, including for an unregistered layer. A layer added after
construction wraps the configured stack. An explicit replacement of the base
context, where exposed, takes precedence over the configured base context and
reapplies the layers to the replacement.

## Option values and errors

OpenDAL returns `Unsupported` for an unregistered service scheme, a context
scope with no registered provider, or an unregistered name in `layers`.
Built-in context and layer factories ignore unrecognized fields. Service options
and extensions follow their component's own option contract.

Omitting an optional field leaves its default or inherited behavior. A supplied
empty string reaches the component as a value. For example, the reqwest HTTP
provider rejects empty strings for its recognized fields. The built-in retry
delays (`min_delay`, `max_delay`), timeout layer deadlines (`timeout`,
`io_timeout`), and supported HTTP provider timeouts (`connect_timeout`,
`read_timeout`, `timeout` for reqwest) accept positive durations such as `500ms`
or `PT1S`. Empty, zero, negative, or otherwise invalid duration values fail with
`ConfigInvalid` when the option is supported. Missing required fields and
invalid values for other supported built-in options also fail with
`ConfigInvalid`. Available transport options depend on the provider and runtime.

An option that holds a map of strings takes a JSON object whose values are
strings, such as `{"fs.defaultFS":"hdfs://ns"}`. OpenDAL keeps the case of the
keys inside the object. A key that appears more than once fails with
`ConfigInvalid`, even when every occurrence has the same value. Any other JSON
value, including a number, `null`, an array, or a nested object, also fails
with `ConfigInvalid`.

## Trusted input

Pass only trusted operator configuration, including the entire URI and any
extra options. Extra options replace matching keys only, so all other URI
query options still apply. When accepting external object URLs, construct the
configuration from validated, allowlisted components such as the scheme,
bucket or host, and path. Use application-controlled credentials, context
options, and layers.

## Historical decision

- [RFC 9061: Unified operator configuration](https://github.com/apache/opendal/pull/9061)
