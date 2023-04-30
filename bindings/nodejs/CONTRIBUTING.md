# Contributing

- [Setup](#setup)
  - [Using a devcontainer environment](#using-a-devcontainer-environment)
  - [Bring your own toolbox](#bring-your-own-toolbox)
- [Build](#build)
- [Test](#test)

## Setup

Building `nodejs` bindings requires some extra setup.

For small or first-time contributions, we recommend the dev container method. Prefer to do it yourself? That's fine too!

### Using a devcontainer environment

OpenDAL provides a pre-configured [dev container](https://containers.dev/) that could be used in [Github Codespaces](https://github.com/features/codespaces), [VSCode](https://code.visualstudio.com/), [JetBrains](https://www.jetbrains.com/remote-development/gateway/), [JuptyerLab](https://jupyterlab.readthedocs.io/en/stable/). Please pick up your favourite runtime environment.

The fastest way is:

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/apache/incubator-opendal?quickstart=1&machine=standardLinux32gb)

### Bring your own toolbox

The `nodejs` binding requires `Node.js@16+` to be built. We recommend using the latest TLS version for development.

OpenDAL provides a `.node-version` file that specifies the recommended node versions. You can use any compatible tool to install the correct node version, such as [fnm](https://github.com/Schniz/fnm).

Alternatively, you can manually install the LTS node by following these steps:

For Ubuntu and Debian:

```shell
> curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash - && sudo apt-get install -y nodejs
```

For RHEL, CentOS, CloudLinux, Amazon Linux or Fedora:

```shell
> curl -fsSL https://rpm.nodesource.com/setup_lts.x | sudo bash -
```

Afterwards, you will need to enable `corepack` to ensure that `yarn` has been set up correctly:

```shell
> sudo corepack enable
> corepack prepare yarn@stable --activate
```

To verify that everything is working properly, run `yarn --version`:

```shell
> yarn --version
3.4.1
```

## Build

```bash
# Install dependencies.
> yarn
# Build from source.
> yarn build
# Build from source with debug info.
> yarn build:debug
```

## Test

We use [`Cucumber`](https://cucumber.io/) for behavior testing. Refer to [here](https://cucumber.io/docs/guides/overview/) for more information about `Cucumber`.

```bash
> yarn test
............

2 scenarios (2 passed)
12 steps (12 passed)
0m00.055s (executing steps: 0m00.004s)
```
