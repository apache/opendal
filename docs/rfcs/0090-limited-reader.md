- Proposal Name: `limited_reader`
- Start Date: 2022-03-02
- RFC PR: [datafuselabs/opendal#0090](https://github.com/datafuselabs/opendal/pull/0090)
- Tracking Issue: [datafuselabs/opendal#0090](https://github.com/datafuselabs/opendal/issues/0090)

# Summary

Native support for limited reader.

# Motivation

In proposal [object-native-api](./0041-object-native-api.md) we introduced `Reader`, in which we will send request like:

```rust
let op = OpRead {
    path: self.path.to_string(),
    offset: Some(self.current_offset()),
    size: None,
};
```

In this implementation, we depend on http client to drop the request as soon as we stop reading. However, we always read too much extra data which decrease our reading performance.

Here is a benchmark around reading whole file and only read half:




# Guide-level explanation

Explain the proposal as if it was already included in the opendal and you were teaching it to other opendal users. That generally means:

- Introducing new named concepts.
- Explaining the feature mainly in terms of examples.
- Explaining how opendal users should *think* about the feature and how it should impact the way they use opendal. It should explain the impact as concretely as possible.
- If applicable, provide sample error messages, deprecation warnings, or migration guidance.
- If applicable, describe the differences between teaching this to exist opendal users and new opendal users.

# Reference-level explanation

This is the technical portion of the RFC. Explain the design in sufficient detail that:

- Its interaction with other features is clear.
- It is reasonably clear how the feature would be implemented.
- Corner cases are dissected by example.

The section should return to the examples given in the previous section and explain more fully how the detailed proposal makes those examples work.

# Drawbacks

Why should we *not* do this?

# Rationale and alternatives

- Why is this design the best in the space of possible designs?
- What other designs have been considered, and what is the rationale for not choosing them?
- What is the impact of not doing this?

# Prior art

Discuss prior art, both the good and the bad, in relation to this proposal.
A few examples of what this can include are:

- What lessons can we learn from what other communities have done here?

This section is intended to encourage you as an author to think about the lessons from other communities provide readers of your RFC with a fuller picture.
If there is no prior art, that is fine - your ideas are interesting to us, whether they are brand new or an adaptation from other projects.

# Unresolved questions

- What parts of the design do you expect to resolve through the RFC process before this gets merged?
- What related issues do you consider out of scope for this RFC that could be addressed in the future independently of the solution that comes out of this RFC?

# Future possibilities

Think about what the natural extension and evolution of your proposal would be and how it would affect the opendal. Try to use this section as a tool to more fully consider all possible interactions with the project in your proposal.

Also, consider how this all fits into the roadmap for the project.

This is also a good place to "dump ideas", if they are out of scope for the
RFC, you are writing but otherwise related.

If you have tried and cannot think of any future possibilities,
you may state that you cannot think of anything.

Note that having something written down in the future-possibilities section
is not a reason to accept the current or a future RFC; such notes should be
in the section on motivation or rationale in this or subsequent RFCs.
The section merely provides additional information.
