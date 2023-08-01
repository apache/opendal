pub fn main() -> std::io::Result<()> {
    ocaml_build::Sigs::new("src/opendal.ml").generate()
}
