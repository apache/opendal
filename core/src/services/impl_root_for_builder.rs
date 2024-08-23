/// A helper macro to impl root methods for all service builders.
#[macro_export]
macro_rules! impl_root_for_builder {
    ($(#[$attr:meta] $(#[doc = $doc:expr])* $struct_name:path),*$(,)?) => {
      $(
        #[$attr]
        impl $struct_name {
            /// Set root of this backend.
            ///
            /// All operations will happen under this root.
            $(#[doc = $doc])*
            pub fn root(mut self, root: &str) -> Self {
                self.config.root = if root.is_empty() {
                    None
                } else {
                    Some(root.to_string())
                };

                self
            }
        }
    )*
    };

    ($($(#[doc = $doc:expr])* $struct_name:path),* $(,)?) => {
        $(
            impl $struct_name {
                /// Set root of this backend.
                ///
                /// All operations will happen under this root.
                $(#[doc = $doc])*
                pub fn root(mut self, root: &str) -> Self {
                    self.config.root = if root.is_empty() {
                        None
                    } else {
                        Some(root.to_string())
                    };

                    self
                }
            }
    )*
    };
}
