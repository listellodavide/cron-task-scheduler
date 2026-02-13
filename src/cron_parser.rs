use chrono::{DateTime, Utc};
use cron::Schedule;
use std::str::FromStr;

pub struct CronParser {
    schedule: Schedule,
}

impl CronParser {
    pub fn new(expression: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let schedule = Schedule::from_str(expression)?;
        Ok(Self { schedule })
    }

    pub fn next_after(&self, from: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.schedule.after(&from).next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_cron_parsing() {
        let parser = CronParser::new("0 */5 * * * *").unwrap();
        let now = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let next = parser.next_after(now).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2024, 1, 1, 0, 5, 0).unwrap());
    }
}
