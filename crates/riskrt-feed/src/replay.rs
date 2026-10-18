//! Replays a recorded tick file.
//!
//! Format: one tick per line, `seq,ts_ns,quote,value`. Blank lines and lines
//! starting with `#` are skipped, as is a header line starting with `seq`.
//!
//! The whole file is parsed up front, so a bad line is reported before any
//! thread starts and the replay itself does no I/O.

use crate::source::TickSource;
use riskrt_core::{QuoteId, Tick};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

#[derive(Debug)]
pub enum ReplayError {
    Io(io::Error),
    /// 1-based line number and what was wrong with it.
    Parse {
        line: usize,
        reason: &'static str,
    },
}

impl core::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ReplayError::Io(e) => write!(f, "replay i/o error: {e}"),
            ReplayError::Parse { line, reason } => write!(f, "replay line {line}: {reason}"),
        }
    }
}

impl std::error::Error for ReplayError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ReplayError::Io(e) => Some(e),
            ReplayError::Parse { .. } => None,
        }
    }
}

impl From<io::Error> for ReplayError {
    fn from(e: io::Error) -> Self {
        ReplayError::Io(e)
    }
}

#[derive(Clone, Debug)]
pub struct ReplaySource {
    ticks: Vec<Tick>,
    next: usize,
}

impl ReplaySource {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ReplayError> {
        Self::from_reader(BufReader::new(File::open(path)?))
    }

    pub fn from_reader(reader: impl BufRead) -> Result<Self, ReplayError> {
        let mut ticks = Vec::new();
        for (i, line) in reader.lines().enumerate() {
            let line = line?;
            let text = line.trim();
            if text.is_empty() || text.starts_with('#') || text.starts_with("seq") {
                continue;
            }
            let tick = parse_line(text).map_err(|reason| ReplayError::Parse {
                line: i + 1,
                reason,
            })?;
            ticks.push(tick);
        }
        Ok(Self::from_ticks(ticks))
    }

    pub fn from_ticks(ticks: Vec<Tick>) -> Self {
        Self { ticks, next: 0 }
    }

    /// Total ticks in the recording.
    pub fn len(&self) -> usize {
        self.ticks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ticks.is_empty()
    }

    /// One past the highest quote index in the recording. Sizes the conflator.
    pub fn quote_count(&self) -> usize {
        self.ticks
            .iter()
            .map(|t| t.quote.index() + 1)
            .max()
            .unwrap_or(0)
    }
}

impl TickSource for ReplaySource {
    fn next_tick(&mut self) -> Option<Tick> {
        let tick = self.ticks.get(self.next).copied()?;
        self.next += 1;
        Some(tick)
    }
}

fn parse_line(text: &str) -> Result<Tick, &'static str> {
    let mut fields = text.split(',').map(str::trim);
    let mut next = || {
        fields
            .next()
            .ok_or("expected 4 fields: seq,ts_ns,quote,value")
    };
    let seq = next()?
        .parse::<u64>()
        .map_err(|_| "seq is not an unsigned integer")?;
    let ts_ns = next()?
        .parse::<u64>()
        .map_err(|_| "ts_ns is not an unsigned integer")?;
    let quote = next()?
        .parse::<u32>()
        .map_err(|_| "quote is not an unsigned integer")?;
    let value = next()?
        .parse::<f64>()
        .map_err(|_| "value is not a number")?;
    if fields.next().is_some() {
        return Err("expected 4 fields: seq,ts_ns,quote,value");
    }
    Ok(Tick::new(QuoteId(quote), seq, ts_ns, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(text: &str) -> Result<ReplaySource, ReplayError> {
        ReplaySource::from_reader(text.as_bytes())
    }

    #[test]
    fn parses_ticks_in_file_order() {
        let mut r = load("1,1000,0,0.0425\n2,2000,3,0.0431\n").unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r.next_tick(), Some(Tick::new(QuoteId(0), 1, 1000, 0.0425)));
        assert_eq!(r.next_tick(), Some(Tick::new(QuoteId(3), 2, 2000, 0.0431)));
        assert_eq!(r.next_tick(), None);
    }

    #[test]
    fn skips_header_comments_and_blank_lines() {
        let r = load("seq,ts_ns,quote,value\n# recorded 2026-10-12\n\n 1 , 1000 , 0 , 0.04 \n")
            .unwrap();
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn quote_count_is_one_past_the_highest_quote() {
        assert_eq!(load("1,0,4,0.04\n2,0,1,0.04\n").unwrap().quote_count(), 5);
        assert_eq!(load("").unwrap().quote_count(), 0);
    }

    #[test]
    fn bad_value_reports_its_line_number() {
        let err = load("1,1000,0,0.04\n2,2000,0,abc\n").unwrap_err();
        assert!(matches!(err, ReplayError::Parse { line: 2, .. }));
    }

    #[test]
    fn wrong_field_count_is_an_error() {
        assert!(matches!(
            load("1,1000,0\n"),
            Err(ReplayError::Parse { line: 1, .. })
        ));
        assert!(matches!(
            load("1,1000,0,0.04,extra\n"),
            Err(ReplayError::Parse { line: 1, .. })
        ));
    }

    #[test]
    fn missing_file_is_an_io_error() {
        let err = ReplaySource::from_path("/nonexistent/riskrt/ticks.csv").unwrap_err();
        assert!(matches!(err, ReplayError::Io(_)));
    }
}
