mod instant;
pub use self::instant::Instant;

mod interval;
pub use self::interval::{Interval, MissedTickBehavior, interval, interval_at};
