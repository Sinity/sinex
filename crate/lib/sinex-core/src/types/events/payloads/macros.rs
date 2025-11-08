macro_rules! assign_setter {
    ($(#[$meta:meta])* $name:ident, $field:ident, $ptype:ty) => {
        $(#[$meta])*
        pub fn $name(mut self, value: $ptype) -> Self {
            self.$field = value;
            self
        }
    };
}

macro_rules! option_setter {
    ($(#[$meta:meta])* $name:ident, $field:ident, $ptype:ty) => {
        $(#[$meta])*
        pub fn $name(mut self, value: $ptype) -> Self {
            self.$field = Some(value);
            self
        }
    };
}

macro_rules! assign_into_setter {
    ($(#[$meta:meta])* $name:ident, $field:ident, $ptype:ty) => {
        $(#[$meta])*
        pub fn $name(mut self, value: $ptype) -> Self {
            self.$field = value.into();
            self
        }
    };
}
