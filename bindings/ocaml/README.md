# OpenDAL OCaml Binding (WIP)

![](https://img.shields.io/badge/status-unreleased-red)

## Requirements

* OCaml version > 4.03 and < 5.0.0


## Contributing


### Setup
We recommend using `OPAM the OCaml Package Manager` to install and manage the OCaml environment.

#### Install OPAM

The quickest way to get the latest opam up and working is to run this script:
```bash
bash -c "sh <(curl -fsSL https://raw.githubusercontent.com/ocaml/opam/master/shell/install.sh)"
```
Similarly, you can also use your distribution's package manager to install

##### Arch
```bash
pacman -S opam
```
##### Debian | Ubuntu
```bash
apt-get install opam
```

#### Init OPAM
*Do not put sudo in front of any opam commands. That would break your OCaml installation.*

After Installing OPAM, we need to initialize it

For the general case, we can execute 
```bash
opam init --bare -a -y
```
If you are using WSL1 on windows, run:
```bash
opam init --bare -a -y --disable-sandboxing
```



#### Create OPAM Switch
Using opam, we can have multiple versions of ocaml at the same time; this is called switch. 

Due to the upstream `ocaml-rs`, we currently do not support OCaml5, and recommend using the latest version of OCaml4
We can create use this command:

```
opam switch create opendal-ocaml4.14 ocaml-base-compiler.4.14.0

eval $(opam env)
```
#### Install OPAM Package

OpenDAL does not depend on opam package except `ounit2` for testing.
However, to facilitate development in an IDE such as vscode, it is usually necessary to install the following content
```bash
opam install -y utop odoc ounit2 ocaml-lsp-server ocamlformat ocamlformat-rpc
```
### Build

```bash
cd bindings/ocaml
dune build
```

### Test
To execute unit tests, we can simply use the following command:
```bash
cd bindings/ocaml
dune test
```
