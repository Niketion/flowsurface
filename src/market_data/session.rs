//! Trading session resolution for time-based data requirements.
//!
//! Provides utilities for resolving trading sessions (Asian, London, NewYork,
//! Daily, Custom) to UTC time ranges. Used by charts and indicators that need
//! to fetch data for specific market sessions.
//!
//! NOTE: This module is planned for SVP (Session Volume Profile) and future
//! session-based indicators. Items are currently unused but retained for that purpose.

#![allow(dead_code)] // SVP readiness — session types and resolvers will be used by SVP

use super::range::MarketDataRange;
use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Timelike, Utc};
use exchange::UnixMs;

/// Trading session definition.
///
/// Sessions define recurring time windows that can be resolved to concrete
/// UTC time ranges for data fetching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Session {
    /// Asian session: 00:00-08:00 UTC
    Asian,
    /// London session: 08:00-16:00 UTC
    London,
    /// New York session: 13:00-21:00 UTC
    NewYork,
    /// Full day: 00:00-24:00 UTC
    Daily,
    /// Custom session with user-defined hours (UTC)
    Custom {
        /// Start hour (0-23)
        from_hour: u8,
        /// End hour (0-23, exclusive)
        to_hour: u8,
    },
}

/// Default trading sessions with their UTC boundaries.
pub const SESSION_BOUNDARIES: &[(Session, u8, u8)] = &[
    (Session::Asian, 0, 8),
    (Session::London, 8, 16),
    (Session::NewYork, 13, 21),
    (Session::Daily, 0, 24),
];

impl Session {
    /// Get the start and end hours for this session (UTC).
    pub fn hours(&self) -> (u8, u8) {
        match self {
            Session::Asian => (0, 8),
            Session::London => (8, 16),
            Session::NewYork => (13, 21),
            Session::Daily => (0, 24),
            Session::Custom { from_hour, to_hour } => (*from_hour, *to_hour),
        }
    }

    /// Resolve this session to a concrete UTC time range for a given date.
    ///
    /// The returned range is [session_start, session_end) in UTC milliseconds.
    pub fn resolve_range(&self, date: NaiveDate) -> MarketDataRange {
        let (from_hour, to_hour) = self.hours();

        let from_time = NaiveTime::from_hms_opt(from_hour as u32, 0, 0).unwrap_or_default();
        let to_time = NaiveTime::from_hms_opt(to_hour as u32, 0, 0).unwrap_or_default();

        let from_dt = date.and_time(from_time);
        let to_dt = if to_hour >= 24 {
            date.succ_opt()
                .unwrap_or(date)
                .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap_or_default())
        } else {
            date.and_time(to_time)
        };

        let from_utc = Utc.from_utc_datetime(&from_dt);
        let to_utc = Utc.from_utc_datetime(&to_dt);

        MarketDataRange::new_unchecked(
            UnixMs::new(from_utc.timestamp_millis() as u64),
            UnixMs::new(to_utc.timestamp_millis() as u64),
        )
    }

    /// Get the session start time for the current date and time.
    ///
    /// If we're currently within the session, returns the session start for today.
    /// If we're before the session, returns the session start for today.
    /// If we're after the session, returns the session start for tomorrow.
    pub fn current_or_next_session_start(&self, now: DateTime<Utc>) -> UnixMs {
        let (from_hour, _to_hour) = self.hours();
        let current_hour = now.hour() as u8;
        let today = now.date_naive();

        if current_hour >= from_hour {
            // We're at or past the session start today
            // Check if we're still within the session
            let (_, to_hour) = self.hours();
            if current_hour < to_hour || to_hour >= 24 {
                // Currently in session
                self.resolve_range(today).from
            } else {
                // Past session, get tomorrow's session
                if let Some(tomorrow) = today.succ_opt() {
                    self.resolve_range(tomorrow).from
                } else {
                    self.resolve_range(today).from
                }
            }
        } else {
            // Before session today
            self.resolve_range(today).from
        }
    }

    /// Get the current session start if we're within the session, or the most
    /// recent session start if we just left it.
    ///
    /// This is useful for Volume Bubbles which need to know the "current session"
    /// even if the session just ended.
    pub fn current_session_start_ms(&self, now: DateTime<Utc>) -> UnixMs {
        let (from_hour, _to_hour) = self.hours();
        let current_hour = now.hour() as u8;
        let today = now.date_naive();

        if current_hour >= from_hour {
            // At or past session start
            self.resolve_range(today).from
        } else {
            // Before session, use yesterday's session
            if let Some(yesterday) = today.pred_opt() {
                self.resolve_range(yesterday).from
            } else {
                self.resolve_range(today).from
            }
        }
    }

    /// Display name for UI and logging.
    pub fn display_name(&self) -> String {
        match self {
            Session::Asian => "Asian".to_string(),
            Session::London => "London".to_string(),
            Session::NewYork => "New York".to_string(),
            Session::Daily => "Daily".to_string(),
            Session::Custom { from_hour, to_hour } => {
                format!("Custom ({from_hour}:00-{to_hour}:00 UTC)")
            }
        }
    }

    /// Parse a session from a string name.
    pub fn parse(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "asian" => Some(Session::Asian),
            "london" => Some(Session::London),
            "new_york" | "newyork" | "ny" => Some(Session::NewYork),
            "daily" => Some(Session::Daily),
            _ => None,
        }
    }
}

impl std::fmt::Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.display_name())
    }
}

/// Resolves sessions to time ranges and provides utilities for
/// determining which session a timestamp belongs to.
pub struct SessionResolver;

impl SessionResolver {
    /// Get all standard sessions.
    pub fn standard_sessions() -> &'static [Session] {
        &[
            Session::Asian,
            Session::London,
            Session::NewYork,
            Session::Daily,
        ]
    }

    /// Determine which session a timestamp belongs to.
    ///
    /// Returns the first matching session (Asian < London < NewYork).
    /// A timestamp can belong to multiple sessions (e.g., 13:00-16:00 UTC
    /// is both London and NewYork).
    pub fn session_for_timestamp(ts: UnixMs) -> Option<Session> {
        let dt = DateTime::from_timestamp_millis(ts.as_u64() as i64)?;
        let hour = dt.hour() as u8;

        for &(session, from, to) in SESSION_BOUNDARIES {
            if hour >= from && hour < to {
                return Some(session);
            }
        }

        Some(Session::Daily)
    }

    /// Get all sessions that a timestamp belongs to.
    pub fn sessions_for_timestamp(ts: UnixMs) -> Vec<Session> {
        let Some(dt) = DateTime::from_timestamp_millis(ts.as_u64() as i64) else {
            return vec![Session::Daily];
        };
        let hour = dt.hour() as u8;

        let mut sessions = Vec::new();
        for &(session, from, to) in SESSION_BOUNDARIES {
            if session == Session::Daily {
                continue; // Skip daily, it's always true
            }
            if hour >= from && hour < to {
                sessions.push(session);
            }
        }

        if sessions.is_empty() {
            sessions.push(Session::Daily);
        }

        sessions
    }

    /// Resolve a session for a specific date to a concrete time range.
    pub fn resolve(session: Session, date: NaiveDate) -> MarketDataRange {
        session.resolve_range(date)
    }

    /// Resolve the "current" session based on the given timestamp.
    ///
    /// If the timestamp is within a session, returns that session's range for today.
    /// If between sessions, returns the next session's range.
    pub fn resolve_current(ts: UnixMs) -> (Session, MarketDataRange) {
        let dt = DateTime::from_timestamp_millis(ts.as_u64() as i64).unwrap_or_default();
        let date = dt.date_naive();

        // Find the session we're currently in or the next one
        for &(session, from, to) in SESSION_BOUNDARIES {
            if session == Session::Daily {
                continue;
            }
            let hour = dt.hour() as u8;
            if hour >= from && hour < to {
                return (session, session.resolve_range(date));
            }
        }

        // Not in any specific session, return Daily
        (Session::Daily, Session::Daily.resolve_range(date))
    }

    /// Get the session range for "today" for a given session type.
    pub fn today(session: Session) -> MarketDataRange {
        let today = Utc::now().date_naive();
        session.resolve_range(today)
    }

    /// Get the session range for a session relative to now.
    ///
    /// If we're in the session, returns the current session.
    /// If we're past the session, returns the next occurrence.
    pub fn current_or_next(session: Session) -> MarketDataRange {
        let now = Utc::now();
        let start = session.current_or_next_session_start(now);
        let (_, to_hour) = session.hours();
        let duration_hours = if to_hour >= 24 {
            24 - session.hours().0 as u64
        } else {
            (to_hour - session.hours().0) as u64
        };
        let end = UnixMs::new(start.as_u64() + duration_hours * 3600 * 1000);

        MarketDataRange::new_unchecked(start, end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_session_hours() {
        assert_eq!(Session::Asian.hours(), (0, 8));
        assert_eq!(Session::London.hours(), (8, 16));
        assert_eq!(Session::NewYork.hours(), (13, 21));
        assert_eq!(Session::Daily.hours(), (0, 24));
    }

    #[test]
    fn test_session_resolve_range() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();

        let asian = Session::Asian.resolve_range(date);
        assert_eq!(asian.duration_ms(), 8 * 3600 * 1000); // 8 hours

        let london = Session::London.resolve_range(date);
        assert_eq!(london.duration_ms(), 8 * 3600 * 1000);

        let ny = Session::NewYork.resolve_range(date);
        assert_eq!(ny.duration_ms(), 8 * 3600 * 1000);

        let daily = Session::Daily.resolve_range(date);
        assert_eq!(daily.duration_ms(), 24 * 3600 * 1000);
    }

    #[test]
    fn test_session_custom() {
        let custom = Session::Custom {
            from_hour: 6,
            to_hour: 18,
        };
        assert_eq!(custom.hours(), (6, 18));

        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let range = custom.resolve_range(date);
        assert_eq!(range.duration_ms(), 12 * 3600 * 1000);
    }

    #[test]
    fn test_session_display() {
        assert_eq!(Session::Asian.display_name(), "Asian");
        assert_eq!(Session::London.display_name(), "London");
        assert_eq!(Session::NewYork.display_name(), "New York");

        let custom = Session::Custom {
            from_hour: 6,
            to_hour: 18,
        };
        assert_eq!(custom.display_name(), "Custom (6:00-18:00 UTC)");
    }

    #[test]
    fn test_session_parse() {
        assert_eq!(Session::parse("asian"), Some(Session::Asian));
        assert_eq!(Session::parse("London"), Some(Session::London));
        assert_eq!(Session::parse("NY"), Some(Session::NewYork));
        assert_eq!(Session::parse("new_york"), Some(Session::NewYork));
        assert_eq!(Session::parse("daily"), Some(Session::Daily));
        assert_eq!(Session::parse("unknown"), None);
    }

    #[test]
    fn test_session_resolver_standard_sessions() {
        let sessions = SessionResolver::standard_sessions();
        assert_eq!(sessions.len(), 4);
    }

    #[test]
    fn test_session_resolver_today() {
        let range = SessionResolver::today(Session::Asian);
        assert_eq!(range.duration_ms(), 8 * 3600 * 1000);
    }
}
