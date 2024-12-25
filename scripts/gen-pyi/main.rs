use std::fs;

use syn::Ident;

fn main() {
    let services = vec![("s3", "S3Config", "../../core/src/services/s3/config.rs")];
    let mut s = fs::read_to_string("python.tmpl").expect("failed to open python template file");

    for (service, struct_name, filename) in services {
        let src = fs::read_to_string(&filename).expect("unable to read file");
        let syntax = syn::parse_file(&src).expect("unable to parse file");

        // Debug impl is available if Syn is built with "extra-traits" feature.
        for item in syntax.items.iter() {
            let item = match item {
                syn::Item::Struct(item_type) => item_type,
                _ => {
                    continue;
                }
            };

            match item.ident.span().source_text() {
                None => {
                    continue;
                }
                Some(s) => {
                    if s != struct_name {
                        println!("{} {}", s, s == struct_name);
                        continue;
                    }
                }
            }

            s.push_str(format!("    @overload\n").as_str());
            s.push_str(format!("    def __init__(self,\n").as_str());
            s.push_str(format!(r#"scheme: Literal["{service}"],"#).as_str());
            s.push_str(format!("\n*,\n").as_str());

            for f in item.fields.iter() {
                s.push_str(
                    f.ident
                        .clone()
                        .unwrap()
                        .span()
                        .source_text()
                        .unwrap()
                        .as_ref(),
                );
                s.push_str(": ");
                let pyi_t = py_type(f.ty.clone());
                s.push_str(pyi_t.as_ref());
                s.push_str(" = ...,\n");
            }

            s.push_str(")->None:...\n");
        }
    }

    s.push_str("    @overload\n    def __init__(self, scheme, **kwargs: str) -> None: ...\n");

    fs::write(r"../../bindings/python/python/opendal/__base.pyi", s)
        .expect("failed to write result to file");
}

fn py_type(t: syn::Type) -> String {
    let p = match t {
        _ => {
            return "str".into();
        }
        syn::Type::Path(p) => p,
    };

    match p.path.get_ident() {
        Some(idnt) => {
            if idnt.span().unwrap().source_text().unwrap() == "bool" {
                return "_bool".into();
            }

            return "str".into();
        }
        _ => {
            return "str".into();
        }
    };
}
