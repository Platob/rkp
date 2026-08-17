//! The JavaScript error classes the core's errors map onto.
//!
//! The classes themselves are ordinary JavaScript, declared in `js/index.js`
//! and handed to the addon once at load time by `registerErrorClasses`.  That
//! keeps `AvroError` and its three subclasses real, subclassable `Error`s with
//! a working `instanceof`, while the addon still decides which one every
//! failure is.
//!
//! Registration also records the environment the classes came from, so the
//! conversion code deep inside `convert.rs` can raise a typed error without
//! threading an `Env` through every helper.  If registration never happened —
//! someone loaded the generated `binding.js` directly rather than the package
//! — every helper degrades to a plain napi error that still carries the class
//! name and the message.

use std::cell::RefCell;
use std::ptr;

use napi::bindgen_prelude::Unknown;
use napi::{Env, Error, JsValue, Status, sys};

/// Which JavaScript class one failure is reported as.
#[derive(Debug, Clone, Copy)]
pub enum Class {
    /// `AvroError`, the base class and the home of container failures.
    Base,
    /// `AvroSchemaError`.
    Schema,
    /// `AvroEncodeError`.
    Encode,
    /// `AvroDecodeError`.
    Decode,
}

impl Class {
    fn slot(self) -> usize {
        match self {
            Class::Base => 0,
            Class::Schema => 1,
            Class::Encode => 2,
            Class::Decode => 3,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Class::Base => "AvroError",
            Class::Schema => "AvroSchemaError",
            Class::Encode => "AvroEncodeError",
            Class::Decode => "AvroDecodeError",
        }
    }
}

struct Registry {
    env: sys::napi_env,
    constructors: [sys::napi_ref; 4],
}

thread_local! {
    static REGISTRY: RefCell<Option<Registry>> = const { RefCell::new(None) };
}

/// Register the four JavaScript error constructors the addon throws.
///
/// `js/index.js` calls this once, at require time.  Calling it again replaces
/// the constructors, which is what reloading the module needs.
pub fn register(
    env: &Env,
    base: Unknown<'_>,
    schema: Unknown<'_>,
    encode: Unknown<'_>,
    decode: Unknown<'_>,
) -> napi::Result<()> {
    let mut constructors = [ptr::null_mut(); 4];
    for (slot, constructor) in constructors.iter_mut().zip([base, schema, encode, decode]) {
        let status = unsafe { sys::napi_create_reference(env.raw(), constructor.raw(), 1, slot) };
        if status != sys::Status::napi_ok {
            return Err(Error::new(
                Status::from(status),
                "failed to hold on to an Avro error constructor".to_string(),
            ));
        }
    }
    REGISTRY.with_borrow_mut(|registry| {
        if let Some(previous) = registry.take() {
            for reference in previous.constructors {
                unsafe { sys::napi_delete_reference(previous.env, reference) };
            }
        }
        *registry = Some(Registry {
            env: env.raw(),
            constructors,
        });
    });
    Ok(())
}

fn construct(class: Class, message: &str) -> Option<Error> {
    REGISTRY.with_borrow(|registry| {
        let registry = registry.as_ref()?;
        let env = registry.env;
        unsafe {
            let mut callable = ptr::null_mut();
            if sys::napi_get_reference_value(
                env,
                registry.constructors[class.slot()],
                &mut callable,
            ) != sys::Status::napi_ok
            {
                return None;
            }
            let mut reason = ptr::null_mut();
            if sys::napi_create_string_utf8(
                env,
                message.as_ptr().cast(),
                message.len() as isize,
                &mut reason,
            ) != sys::Status::napi_ok
            {
                return None;
            }
            let mut instance = ptr::null_mut();
            if sys::napi_new_instance(env, callable, 1, &reason, &mut instance)
                != sys::Status::napi_ok
            {
                return None;
            }
            Some(Error::from(Unknown::from_raw_unchecked(env, instance)))
        }
    })
}

/// Build one JavaScript error of the given class.
pub fn error(class: Class, message: impl Into<String>) -> Error {
    let message = message.into();
    construct(class, &message).unwrap_or_else(|| {
        // Nobody registered the classes; keep the message, lose the class.
        Error::new(
            Status::GenericFailure,
            format!("{}: {message}", class.name()),
        )
    })
}

/// Map one core error onto its JavaScript class.
pub fn from_core(failure: rkp_avro::Error) -> Error {
    let class = match failure {
        rkp_avro::Error::Schema(_) => Class::Schema,
        rkp_avro::Error::Encode(_) => Class::Encode,
        rkp_avro::Error::Decode(_) => Class::Decode,
        rkp_avro::Error::Container(_) => Class::Base,
    };
    error(class, failure.message())
}

/// Report a malformed or unusable schema.
pub fn schema(message: impl Into<String>) -> Error {
    error(Class::Schema, message)
}

/// Report a value that cannot be encoded against its schema.
pub fn encode(message: impl Into<String>) -> Error {
    error(Class::Encode, message)
}

/// Report encoded data that is truncated or inconsistent with its schema.
pub fn decode(message: impl Into<String>) -> Error {
    error(Class::Decode, message)
}

/// Report a container operation that is impossible or unsafe.
pub fn container(message: impl Into<String>) -> Error {
    error(Class::Base, message)
}
