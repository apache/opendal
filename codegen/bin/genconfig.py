from pathlib import Path

from jinja2 import Environment, FileSystemLoader

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
        print(tmpl.render(configs=[S3]))
        f.write(tmpl.render(configs=[S3]))
