# Introduction

Rust is a systems programming language that is known for its performance, memory safety, and concurrency features. OpenDAL(**Open D**ata **A**ccess **L**ayer) is a Rust project to access data freely, painlessly and efficiently.

This examples documentation is organized in a progressive manner, with each section building upon the previous one. Each section is located in a different folder and contains both documentation and a stand-alone project that can be run independently.

The following will briefly introduce dev environment set up, the Rust project structure, some important files, and how to quickly create a project. This is prepared for those who are not familiar with Rust. If you are familiar with Rust, you can skip this section.


# Environment set up

There need rust toolchain in your local environment, if you haven't set it up, you can refer [here](https://github.com/apache/incubator-opendal/blob/main/CONTRIBUTING.md#bring-your-own-toolbox) to figure it out.


# Rust Project Structure

A typical Rust project follows a specific directory structure. Here's an overview of the main directories and files you'll encounter in a Rust project:

- src/: This directory contains the source code for your Rust project. It typically includes a main.rs file, which is the entry point for your application.

- Cargo.toml: This file contains metadata about your project, such as its name, version, and dependencies. It also specifies the build configuration for your project.

## Cargo.toml

Cargo is Rust's package manager and build tool. It uses the Cargo.toml file to manage dependencies and build configurations for your project.

Here's an example Cargo.toml file for a Rust project:

```toml
[package]
name = "my_project"
version = "0.1.0"
authors = ["Your Name <your.email@example.com>"]
edition = "2018"

[dependencies]
serde = { version = "1.0", features = ["derive"] }
```
## main.rs
The main.rs file is the entry point for your Rust application. It typically contains the `main()` function, which is the starting point for your program.


# Create project

Once you have configured the Rust environment, you can use `cargo new peoject-day01 --bin` to create a example project quickly.