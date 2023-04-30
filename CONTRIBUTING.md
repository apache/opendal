# Contributing

First, thank you for contributing to OpenDAL! The goal of this document is to provide everything you need to start contributing to OpenDAL. The following TOC is sorted progressively, starting with the basics and expanding into more specifics.

- [Your First Contribution](#your-first-contribution)
- [Workflow](#workflow)
  - [Git Branches](#git-branches)
  - [GitHub Pull Requests](#github-pull-requests)
    - [Title](#title)
    - [Reviews & Approvals](#reviews--approvals)
    - [Merge Style](#merge-style)
    - [CI](#ci)
- [Next steps](#next-steps)
- [Code of Conduct](#code-of-conduct)

## Your First Contribution

1. Ensure your change has an issue! Find an [existing issue](https://github.com/apache/incubator-opendal/issues) or [open a new issue](https://github.com/apache/incubator-opendal/issues/new).
1. [Fork the OpenDAL repository](https://github.com/apache/incubator-opendal/fork) in your own GitHub account.
1. [Create a new Git branch](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-and-deleting-branches-within-your-repository).
1. Make your changes.
1. [Submit the branch as a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) to the main OpenDAL repo. An OpenDAL team member should comment and/or review your pull request within a few days. Although, depending on the circumstances, it may take longer.

## Workflow

### Git Branches

*All* changes must be made in a branch and submitted as [pull requests](#github-pull-requests). OpenDAL does not adopt any type of branch naming style, but please use something descriptive of your changes.

### GitHub Pull Requests

Once your changes are ready you must submit your branch as a [pull request](https://github.com/apache/incubator-opendal/pulls).

#### Title

The pull request title must follow the format outlined in the [conventional commits spec](https://www.conventionalcommits.org). [Conventional commits](https://www.conventionalcommits.org) is a standardized format for commit messages. OpenDAL only requires this format for commits on the `main` branch. And because OpenDAL squashes commits before merging branches, this means that only the pull request title must conform to this format.

The following are all good examples of pull request titles:

```text
feat(services/gcs): Add start-after support for list
docs: add hdfs classpath related troubleshoot
ci: Mark job as skipped if owner is not apache
fix(services/s3): Ignore prefix if it's empty
refactor: Polish the implementation of webhdfs
```

#### Reviews & Approvals

All pull requests should be reviewed by at least one OpenDAL committer.

#### Merge Style

All pull requests are squash merged. We generally discourage large pull requests that are over 300-500 lines of diff. If you would like to propose a change that is larger we suggest coming onto our [Discussions](https://github.com/apache/incubator-opendal/discussions) and discuss it with us. This way we can talk through the solution and discuss if a change that large is even needed! This will produce a quicker response to the change and likely produce code that aligns better with our process.

### CI

Currently, OpenDAL uses GitHub Actions to run tests. The workflows are defined in `.github/workflows`.

## Next steps

Visiting **[DEVELOPING.md](DEVELOPING.md)** to prepare Everything necessary to develop.

## Code of Conduct

We expect all community members to follow our [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).
