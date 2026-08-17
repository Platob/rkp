//! The enums that generalize over every implementation of one contract.
//!
//! A trait says what an implementation must do; the enum here says which
//! implementations exist. Holding one of these means holding "some handle" or
//! "some media" as a concrete value - no trait object, no generic parameter -
//! which is what lets a location, a listing, or a binding pass an
//! implementation around without knowing which one it is.
//!
//! - [`Holder`] names every [`crate::io::IOBase`] implementation.
//! - [`Codec`] names every transparent content coding applied to a handle.
//!
//! It also owns [`Value`], the one native value the whole project speaks: every
//! codec parses into it, every field validates it, and every binding converts
//! its own objects to it. Its scalar behavior is split by what it describes -
//! `value` for the shape and the ordering, `decimal` and `temporal` for the
//! kinds that carry a scale or a unit, `inference` for the datatype a value
//! already names, and `typed` for the pairing that carries a datatype a null
//! could not have named on its own.
//! - [`Media`] names every record encoding bound to a handle.
//! - [`RecordOptions`] names every encoding's read and write settings.
//!
//! Each one delegates the whole contract to the variant it holds, so code
//! written against the enum behaves exactly as code written against the
//! implementation would.

mod codec;
mod decimal;
mod holder;
mod inference;
#[cfg(feature = "arrow")]
mod media;
#[cfg(feature = "arrow")]
mod options;
mod temporal;
mod text;
mod typed;
pub mod value;

pub use codec::Codec;
pub use holder::Holder;
#[cfg(feature = "arrow")]
pub use media::Media;
#[cfg(feature = "arrow")]
pub use options::{IORecordOptions, RecordOptions};
pub use text::Text;
pub use typed::TypedValue;
pub use value::{Children, Float, Value};
