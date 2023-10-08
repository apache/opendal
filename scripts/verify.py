#!/usr/bin/env python3

import subprocess
import sys
import os

BASE_DIR = os.getcwd()


def check_rust():
    try:
        subprocess.run(['cargo', '--version'], check=True)
        return True
    except FileNotFoundError:
        return False
    except Exception as e:
        raise Exception("Check rust met unexpected error", e)


def check_java():
    try:
        subprocess.run(['java', '--version'], check=True)
        return True
    except FileNotFoundError:
        return False
    except Exception as e:
        raise Exception("Check java met unexpected error", e)


def build_core():
    print("Start building opendal core")

    subprocess.run(['cargo', 'build'], check=True)


def build_java_binding():
    print("Start building opendal java binding")

    # change to java binding directory
    os.chdir('bindings/java')

    subprocess.run(['./mvnw', 'clean', 'install', '-DskipTests=true'], check=True)

    # change back to base directory
    os.chdir(BASE_DIR)


def main():
    if not check_rust():
        print("Cargo is not found, please check if rust development has been setup correctly")
        print("Visit https://www.rust-lang.org/tools/install for more information")
        sys.exit(1)

    build_core()

    if check_java():
        build_java_binding()
    else:
        print("Java is not found, skipped building java binding")


if __name__ == "__main__":
    main()
