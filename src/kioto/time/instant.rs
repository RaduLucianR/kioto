#[derive(Debug, PartialEq, PartialOrd, Copy, Clone)]
pub struct Instant {
    instant: std::time::Instant,
}

impl Instant {
    pub fn now() -> Instant {
        Instant {
            instant: std::time::Instant::now(),
        }
    }

    pub fn from_std(std_instant: std::time::Instant) -> Instant {
        Instant {
            instant: std_instant,
        }
    }
}

impl std::ops::Add<std::time::Duration> for Instant {
    type Output = Instant;

    fn add(self, other: std::time::Duration) -> Instant {
        Instant::from_std(self.instant + other)
    }
}
