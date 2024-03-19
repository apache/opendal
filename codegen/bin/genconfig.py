from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from codegen import safeexec
from codegen.config import S3

if __name__ == '__main__':
    codegen = Path(__file__).resolve().parent.parent
    env = Environment(
        loader=FileSystemLoader(codegen / 'template'),
    )
    tmpl = env.get_template("config.rs.j2")

    opendal = codegen.parent
    rust = opendal / 'core' / 'src' / 'services' / 'config.rs'
    with rust.open('w') as f:
        template = Path(tmpl.filename).relative_to(opendal)
        output = rust.relative_to(opendal)
        print(f'Generating Rust files from templates in {template} to {output}')
        f.write(tmpl.render(configs=[S3]))

    rustfmt = safeexec.lookup('rustfmt')
    safeexec.run(rustfmt, str(rust), verbose=True)
