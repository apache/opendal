#!/usr/bin/env python3

from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from pathlib import Path
import shutil
import subprocess


def classifier_to_target(classifier: str) -> str:
    if classifier == 'osx-aarch_64':
        return 'aarch64-apple-darwin'
    raise Exception(f'unsupproted classifier {classifier}')


def get_cargo_artifact_name(classifier: str) -> str:
    if classifier == 'osx-aarch_64':
        return 'libopendal_java.dylib'
    raise Exception(f'unsupproted classifier {classifier}')

if __name__ == '__main__':
    binding_root = Path(__file__).parent.parent

    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('--classifier', type=str, required=True)
    args = parser.parse_args()

    cmd = ['cargo', 'build', '--color=always', '--release']

    target = classifier_to_target(args.classifier)
    if target:
        subprocess.run(['rustup', 'target', 'add', target], cwd=binding_root, check=True)
        cmd += ['--target', target]

    output = binding_root / 'target'
    Path(output).mkdir(exist_ok=True, parents=True)
    cmd += ['--target-dir', output]

    subprocess.run(cmd, cwd=binding_root, check=True)

    artifact = get_cargo_artifact_name(args.classifier)
    src = output / 'release' / artifact
    dst = output / 'native' / args.classifier / artifact
    dst.parent.mkdir(exist_ok=True, parents=True)
    shutil.copy2(src, dst)
