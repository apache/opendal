# Contributing

First, thank you for contributing to OpenDAL! The goal of this document is to provide everything you need to start contributing to OpenDAL. The following TOC is sorted progressively, starting with the basics and expanding into more specifics.

- [Contributing](#contributing)
  - [Your First Contribution](#your-first-contribution)
  - [AI-Assisted Contributions](#ai-assisted-contributions)
    - [Accountability and Agent Participation](#accountability-and-agent-participation)
    - [AI Usage Statement](#ai-usage-statement)
    - [Bug Reports](#bug-reports)
    - [Code Changes](#code-changes)
    - [AI-Assisted Maintenance](#ai-assisted-maintenance)
  - [Workflow](#workflow)
    - [Git Branches](#git-branches)
    - [GitHub Pull Requests](#github-pull-requests)
      - [Title](#title)
      - [Reviews \& Approvals](#reviews--approvals)
      - [Merge Style](#merge-style)
    - [CI](#ci)
  - [Setup](#setup)
    - [Using a dev container environment](#using-a-dev-container-environment)
    - [Bring your own toolbox](#bring-your-own-toolbox)
  - [Code of Conduct](#code-of-conduct)

## Your First Contribution

1. Ensure your change has an issue! Find an [existing issue](https://github.com/apache/opendal/issues) or [open a new issue](https://github.com/apache/opendal/issues/new).
1. [Fork the OpenDAL repository](https://github.com/apache/opendal/fork) in your own GitHub account.
1. [Create a new Git branch](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-and-deleting-branches-within-your-repository).
1. Make your changes.
1. [Submit the branch as a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) to the main OpenDAL repo. An OpenDAL team member should comment and/or review your pull request within a few days. Although, depending on the circumstances, it may take longer.

## AI-Assisted Contributions

Attention is the scarcest resource in an open source community. We appreciate
everyone who gives their attention to OpenDAL, and we ask contributors to
protect the attention of others. These standards apply to every contribution,
whether authored by a human or an AI agent; AI assistance does not lower them.

### Accountability and Agent Participation

OpenDAL welcomes AI-assisted contributions and authorized AI agents
participating in Issues, Pull Requests, Discussions, and code reviews, as long
as every submission has clear human accountability and meets our evidence and
quality standards.

- **Accountable human owner:** Every AI-assisted submission or agent
  interaction must have an identifiable human contributor or maintainer who
  owns its accuracy, licensing compliance, and follow-through. When an agent
  gets stuck, makes unsupported claims, or reaches a design disagreement, the
  accountable human must step in.
- **Transparent AI attribution:** Clearly identify AI-authored comments,
  triage notes, and reviews as AI-generated, including when posted under a
  contributor's or committer's personal GitHub account. Pull requests must
  disclose material AI involvement in the AI Usage Statement below.
- **Verified, high-signal communication:** Do not paste unverified LLM output,
  filler, or repetitive summaries into project conversations. Verify claims
  against the codebase and tests before posting, add concrete technical
  evidence when replying, and stop rather than looping on unresolved
  disagreements.

The accountable human contributor or maintainer, not the AI tool, remains
responsible for the accuracy, relevance, license compliance, and quality of
every submission.

### AI Usage Statement

If AI materially assisted or authored a Pull Request, include the following in
its AI Usage Statement:

- **Harness:** The application or agent framework used to run the model.
- **Model:** The model name or identifier shown by the harness, including its
  version when available.
- **Effort:** The reasoning effort setting used, as named by the harness.
- **Role:** How AI contributed, and any assumptions or unknowns that affect
  review.

List each harness, model, and effort combination that materially contributed.
Keep the statement brief and omit routine command output or information already
provided elsewhere. If AI did not materially assist the PR, write `None`.

### Bug Reports

OpenDAL welcomes the use of AI tools to analyze source code and find bugs. A
source-code hypothesis is not sufficient evidence for a bug report.

- Reproduce the reported behavior against the affected component. For a
  service-specific bug, reproduce it against the actual service being reported.
  A mock server, emulator, or source-code analysis alone does not establish a
  bug in the service.
- Provide the OpenDAL version or commit, the service or component, relevant
  sanitized configuration, minimal reproduction steps, the actual result, and
  the expected result.
- Submit an unverified hypothesis as a
  [Discussion](https://github.com/apache/opendal/discussions) or question, not as
  a confirmed bug.

Do not publish credentials or other sensitive data. Report security concerns
through the process described in [Security](website/community/security.md).

### Code Changes

OpenDAL welcomes AI-assisted bug fixes and feature implementations that solve a
demonstrated problem or meet a concrete use case.

- Link the change to an Issue that establishes the problem or requirement.
- Verify a bug before fixing it, and add tests that demonstrate the failure and
  protect the intended behavior when practical.
- Explain the user-facing, correctness, or maintenance value of the change.
- Keep changes focused on the stated goal; avoid unrelated refactors,
  formatting churn, or speculative abstractions.
- Report only checks and tests that actually ran, and do not weaken or delete
  existing tests or assertions to make CI pass.
- Avoid opening multiple concurrent speculative or low-effort pull requests;
  address review feedback on existing submissions first.

OpenDAL does not accept speculative fixes for unverified bugs or mechanical
refactors without an agreed technical goal. Maintainers may close submissions
without a detailed technical review when they lack real reproduction or
evidence, misrepresent validation, duplicate existing work, expand scope
without consensus, fail to converge after feedback, or lack an accountable
human owner when human follow-up is required.

### AI-Assisted Maintenance

OpenDAL maintainers also use AI agents to help maintain the project, including
issue triage, initial pull request screening, code review, reproduction
assistance, release checks, and routine maintenance changes.

- **Attribution:** Maintenance agents may post through a dedicated bot account
  or under a committer's personal GitHub account with a clear AI label so
  contributors can distinguish automated analysis from a maintainer's human
  judgment or a formal PMC decision.
- **Committer and PMC authority:** Every pull request requires at least one
  human OpenDAL committer to review, approve, and merge it. Public API changes,
  specifications, architectural decisions, security vulnerability
  classifications, and Apache release votes remain under human committer and
  PMC accountability.
- **Human escalation:** Automated triage and review can be wrong. If an AI
  maintenance agent misclassifies or closes an Issue or Pull Request, reply
  with concrete technical evidence and request review from a human maintainer.

## Workflow

### Git Branches

*All* changes must be made in a branch and submitted as [pull requests](#github-pull-requests). OpenDAL does not adopt any type of branch naming style, but please use something descriptive of your changes.

### GitHub Pull Requests

Once your changes are ready you must submit your branch as a [pull request](https://github.com/apache/opendal/pulls).

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

All pull requests must be reviewed, approved, and merged by at least one human
OpenDAL committer. AI agents may assist with initial screening and technical
review, but they do not replace committer approval or merge accountability.

#### Merge Style

All pull requests are squash merged.
We generally discourage large pull requests that are over 300–500 lines of diff.
If you would like to propose a change that is larger, we suggest
coming onto our [Discussions](https://github.com/apache/opendal/discussions) and discussing it with us.
This way we can talk through the solution and discuss if a change that large is even needed!
This will produce a quicker response to the change and likely produce code that aligns better with our process.

### CI

Currently, OpenDAL uses GitHub Actions to run tests. The workflows are defined in `.github/workflows`.

## Setup

For small or first-time contributions, we recommend the dev container method. Prefer to do it yourself? That's fine too!

### Using a dev container environment

OpenDAL provides a pre-configured [dev container](https://containers.dev/)
that could be used in [GitHub Codespaces](https://github.com/features/codespaces),
[VSCode](https://code.visualstudio.com/), [JetBrains](https://www.jetbrains.com/remote-development/gateway/),
[JupyterLab](https://jupyterlab.readthedocs.io/en/stable/).
Please pick up your favourite runtime environment.

The fastest way is:

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/apache/opendal?quickstart=1&machine=standardLinux32gb)

### Bring your own toolbox

OpenDAL is primarily a Rust project. To build OpenDAL, you will need to set up Rust development first. We highly recommend using [rustup](https://rustup.rs/) for the setup process.

For Linux or macOS, use the following command:

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

For Windows, download `rustup-init.exe` from [here](https://win.rustup.rs/x86_64) instead.

Rustup will read OpenDAL's `rust-toolchain.toml` and set up everything else automatically. To ensure that everything works correctly, run `cargo version` under OpenDAL's root directory:

```shell
$ cargo version
cargo 1.91.0 (stable)
```

Some components may require specific setup steps. Please refer to their respective `CONTRIBUTING` documentation for more details.

- [Core](core/CONTRIBUTING.md)
- [Node.js Binding](bindings/nodejs/CONTRIBUTING.md)
- [Python Binding](bindings/python/CONTRIBUTING.md)
- [Java Binding](bindings/java#build)

## Code of Conduct

We expect all community members to follow our [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).
