- Proposal Name: (fill me in with a unique ident, `my_awesome_feature`)
- Start Date: (fill me in with today's date, YYYY-MM-DD)
- RFC PR: [apache/incubator-opendal#0000](https://github.com/apache/incubator-opendal/pull/0000)
- Tracking Issue: [apache/incubator-opendal#0000](https://github.com/apache/incubator-opendal/issues/0000)

# Summary

Add `native_capability` and `full_capability` for `Operator` so that users can make better decisions.

# Motivation

OpenDAL adds `Capability` to let users know whether a service supports a specific feature. However, it is not enough for users to make decisions. But OpenDAL doesn't simply expose services API directly, instead it will try to simulate the behavior to make it more useful.

For examples, `s3` doesn't support seek operations like local fs. But it's a quiet common operation for users. So OpenDAL will try to simulate the behavior by calculate the correct offset and read the data from the offset instead. After this simulation, `s3` service has `read_can_seek` capability now.

As another example, most services like `s3` doesn't support blocking operation, OpenDAL implements a `BlockingLayer` to make it possible. After this implementation, `s3` service has `blocking` capability now.

However, these capabilities alone are insufficient for users to make informed decisions. Take the `s3` service's `blocking` capability as an example. Users are unable to determine whether it is a native capability or not, which may result in them unknowingly utilizing this feature in performance-sensitive scenarios and experiencing significantly poor performance.



# Guide-level explanation


# Reference-level explanation


# Drawbacks


# Rationale and alternatives



# Prior art



# Unresolved questions


# Future possibilities

