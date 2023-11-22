
#[napi]
pub struct Capability(opendal::Capability);

impl Capability {
    pub fn new(op: opendal::Operator) -> Self {
        Self(op.info().full_capability())
    }
}

#[napi]
impl Capability {
    #[napi(getter)]
    pub fn stat(&self) -> bool {
        self.0.stat
    }

    // stat_with_if_match
    #[napi(getter)]
    pub fn stat_with_if_none_match(&self) -> bool {
        self.0.stat_with_if_none_match
    }

    // stat_with_if_none_match
    #[napi(getter)]
    pub fn stat_with_if_match(&self) -> bool {
        self.0.stat_with_if_match
    }

    #[napi(getter)]
    pub fn read(&self) -> bool {
        self.0.read
    }

    #[napi(getter)]
    pub fn write(&self) -> bool {
        self.0.write
    }

    #[napi(getter)]
    pub fn delete(&self) -> bool {
        self.0.delete
    }
}
