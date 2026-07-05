//! Time range types and operations for market data coverage tracking.
//!
//! `MarketDataRange` represents a time interval with inclusive start and
//! exclusive end. Provides operations for merging, splitting, and computing
//! gaps between ranges.

use exchange::UnixMs;

/// A time range for market data, with inclusive start and exclusive end.
///
/// Used for coverage tracking, requirement specification, and fetch planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MarketDataRange {
    /// Inclusive start timestamp (milliseconds since epoch)
    pub from: UnixMs,
    /// Exclusive end timestamp (milliseconds since epoch)
    pub to: UnixMs,
}

impl MarketDataRange {
    /// Create a new range. Returns None if from >= to (invalid range).
    pub fn new(from: UnixMs, to: UnixMs) -> Option<Self> {
        if from < to {
            Some(Self { from, to })
        } else {
            None
        }
    }

    /// Create a range without validation (use when range is known valid).
    pub fn new_unchecked(from: UnixMs, to: UnixMs) -> Self {
        Self { from, to }
    }

    /// Duration of this range in milliseconds.
    pub fn duration_ms(&self) -> u64 {
        self.to.as_u64().saturating_sub(self.from.as_u64())
    }

    /// Check if this range is empty (zero duration).
    pub fn is_empty(&self) -> bool {
        self.from >= self.to
    }

    /// Check if this range contains a timestamp.
    pub fn contains_timestamp(&self, ts: UnixMs) -> bool {
        ts >= self.from && ts < self.to
    }

    /// Check if this range fully contains another range.
    pub fn contains(&self, other: &Self) -> bool {
        self.from <= other.from && self.to >= other.to
    }

    /// Check if this range overlaps with another range.
    pub fn overlaps(&self, other: &Self) -> bool {
        self.from < other.to && other.from < self.to
    }

    /// Compute the intersection of two ranges.
    pub fn intersection(&self, other: &Self) -> Option<Self> {
        let from = self.from.max(other.from);
        let to = self.to.min(other.to);
        Self::new(from, to)
    }

    /// Merge two overlapping or adjacent ranges into one.
    ///
    /// Returns None if the ranges don't overlap or touch.
    pub fn merge(&self, other: &Self) -> Option<Self> {
        if self.overlaps(other) || self.adjacent_to(other) {
            let from = self.from.min(other.from);
            let to = self.to.max(other.to);
            Some(Self { from, to })
        } else {
            None
        }
    }

    /// Check if two ranges are adjacent (no gap between them).
    pub fn adjacent_to(&self, other: &Self) -> bool {
        self.to == other.from || other.to == self.from
    }

    /// Subtract another range from this range, returning 0, 1, or 2 remaining ranges.
    ///
    /// If `other` fully covers `self`, returns empty vec.
    /// If `other` partially overlaps, returns the non-overlapping portion(s).
    pub fn subtract(&self, other: &Self) -> Vec<MarketDataRange> {
        let mut result = Vec::new();

        // No overlap
        if !self.overlaps(other) {
            result.push(*self);
            return result;
        }

        // Left portion (before other starts)
        if self.from < other.from {
            result.push(MarketDataRange {
                from: self.from,
                to: other.from,
            });
        }

        // Right portion (after other ends)
        if self.to > other.to {
            result.push(MarketDataRange {
                from: other.to,
                to: self.to,
            });
        }

        result
    }

    /// Compute the gap between two ranges.
    ///
    /// Returns Some(gap) if there's a gap, None if they overlap or are adjacent.
    pub fn gap_to(&self, other: &Self) -> Option<Self> {
        if self.overlaps(other) || self.adjacent_to(other) {
            return None;
        }

        if self.to < other.from {
            Self::new(self.to, other.from)
        } else {
            Self::new(other.to, self.from)
        }
    }

    /// Split this range at a timestamp.
    ///
    /// Returns (left, right) where left is [from, at) and right is [at, to).
    /// If `at` is outside the range, one of the returned ranges will be None.
    pub fn split_at(&self, at: UnixMs) -> (Option<Self>, Option<Self>) {
        if at <= self.from {
            (None, Some(*self))
        } else if at >= self.to {
            (Some(*self), None)
        } else {
            (
                Some(MarketDataRange {
                    from: self.from,
                    to: at,
                }),
                Some(MarketDataRange {
                    from: at,
                    to: self.to,
                }),
            )
        }
    }

    /// Format as a human-readable string (e.g., "15:30:00 → 15:58:00")
    pub fn format_display(&self) -> String {
        format!(
            "{} → {}",
            crate::connector::fetcher::format_time_short(self.from),
            crate::connector::fetcher::format_time_short(self.to)
        )
    }
}

impl std::fmt::Display for MarketDataRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format_display())
    }
}

/// Utility functions for working with collections of ranges.
///
/// Merge a list of potentially overlapping/adjacent ranges into a sorted,
/// non-overlapping list.
pub fn merge_ranges(ranges: &[MarketDataRange]) -> Vec<MarketDataRange> {
    if ranges.is_empty() {
        return Vec::new();
    }

    let mut sorted: Vec<MarketDataRange> = ranges.to_vec();
    sorted.sort_by_key(|r| r.from);

    let mut merged: Vec<MarketDataRange> = Vec::with_capacity(sorted.len());
    merged.push(sorted[0]);

    for range in &sorted[1..] {
        let last = merged.last_mut().unwrap();
        if let Some(combined) = last.merge(range) {
            *last = combined;
        } else {
            merged.push(*range);
        }
    }

    merged
}

/// Compute the missing ranges by subtracting `covered` from `desired`.
///
/// Returns a list of ranges that are in `desired` but not in `covered`.
pub fn compute_missing(
    desired: MarketDataRange,
    covered: &[MarketDataRange],
) -> Vec<MarketDataRange> {
    let mut remaining = vec![desired];

    for cover in covered {
        let mut next_remaining = Vec::new();
        for r in remaining {
            next_remaining.extend(r.subtract(cover));
        }
        remaining = next_remaining;

        if remaining.is_empty() {
            break;
        }
    }

    remaining
}

/// Check if a range is fully covered by a list of coverage ranges.
pub fn is_fully_covered(range: &MarketDataRange, covered: &[MarketDataRange]) -> bool {
    if covered.is_empty() {
        return false;
    }

    let merged = merge_ranges(covered);
    let mut cursor = range.from;

    for cover in &merged {
        if cover.from > cursor {
            return false; // Gap found
        }
        cursor = cursor.max(cover.to);
        if cursor >= range.to {
            return true; // Fully covered
        }
    }

    false
}

/// Canonicalize a Kline range to align to timeframe boundaries.
///
/// Kline coverage is based on candle open time.
/// Range convention: [from_open, to_open_exclusive).
///
/// - from = floor to timeframe boundary
/// - to = ceil to timeframe boundary
///
/// Returns None if the canonicalized range is empty (duration < 1 timeframe).
pub fn canonicalize_kline_range(
    range: MarketDataRange,
    timeframe_ms: u64,
) -> Option<MarketDataRange> {
    if timeframe_ms == 0 {
        return Some(range);
    }

    let from_ms = range.from.as_u64();
    let to_ms = range.to.as_u64();

    let canonical_from = UnixMs::new(from_ms - (from_ms % timeframe_ms));
    let canonical_to = if to_ms.is_multiple_of(timeframe_ms) {
        UnixMs::new(to_ms)
    } else {
        UnixMs::new(to_ms + (timeframe_ms - (to_ms % timeframe_ms)))
    };

    if canonical_from >= canonical_to {
        log::info!(
            target: "marketdata",
            "MARKETDATA TinyKlineGapSuppressed | range={} reason=below_timeframe",
            range.format_display()
        );
        return None;
    }

    let canonical = MarketDataRange {
        from: canonical_from,
        to: canonical_to,
    };

    if canonical != range {
        log::info!(
            target: "marketdata",
            "MARKETDATA KlineRangeCanonicalized | original={} canonical={}",
            range.format_display(),
            canonical.format_display()
        );
    }

    Some(canonical)
}

/// Add a segment to a vec, merging with any overlapping existing segments.
///
/// This prevents near-duplicate segments (e.g., 1ms differences) from inflating
/// required/completed/delivered segment counts.
///
/// Unlike `merge_ranges`, this only merges truly overlapping segments,
/// not adjacent ones. Adjacent segments (e.g., [100,200) and [200,300))
/// remain separate to preserve split-request segment tracking.
pub fn add_segment_merged(vec: &mut Vec<MarketDataRange>, segment: MarketDataRange) {
    // Check if any existing segment overlaps with the new one
    let overlapping_idx = vec.iter().position(|existing| existing.overlaps(&segment));

    match overlapping_idx {
        Some(idx) => {
            // Merge with the overlapping segment
            let merged = vec[idx].merge(&segment).unwrap_or(segment);
            vec[idx] = merged;
        }
        None => {
            // No overlap, just push
            vec.push(segment);
        }
    }
}

/// Add a segment to a logical required-segment list using dedup, not merge.
///
/// Adjacent segments (e.g., [100,200) and [200,300)) are kept separate so
/// that logical segment counts remain accurate for logging/debug.
///
/// - If `segment` is already fully covered by an existing required segment,
///   it is skipped.
/// - If `segment` fully covers some existing smaller segments, those are
///   removed and `segment` is pushed in their place.
pub fn add_required_segment_dedup(required: &mut Vec<MarketDataRange>, segment: MarketDataRange) {
    // Skip if the segment is already fully covered by an existing entry.
    if compute_missing(segment, required).is_empty() {
        return;
    }

    // Remove any existing segments that are fully covered by the new one.
    required.retain(|existing| !compute_missing(*existing, &[segment]).is_empty());

    required.push(segment);
    required.sort_by_key(|r| r.from);
}

/// Filter out tiny gaps from a list of missing ranges.
///
/// For Trade/TradeHydration features, very small gaps (< `threshold_ms`)
/// are suppressed as they represent tiny tail offsets from completed
/// network fetches and are not worth a separate backfill request.
pub fn filter_tiny_trade_gaps(
    missing: Vec<MarketDataRange>,
    threshold_ms: u64,
) -> Vec<MarketDataRange> {
    let mut suppressed = 0;
    let filtered: Vec<MarketDataRange> = missing
        .into_iter()
        .filter(|range| {
            if range.duration_ms() < threshold_ms {
                suppressed += 1;
                log::info!(
                    target: "marketdata",
                    "MARKETDATA TinyTradeGapSuppressed | range={} duration_ms={}",
                    range.format_display(),
                    range.duration_ms()
                );
                false
            } else {
                true
            }
        })
        .collect();
    if suppressed > 0 {
        log::info!(
            target: "marketdata",
            "MARKETDATA TinyTradeGapFiltered | total_suppressed={}",
            suppressed
        );
    }
    filtered
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(v: u64) -> UnixMs {
        UnixMs::new(v)
    }

    #[test]
    fn test_range_basics() {
        let r = MarketDataRange::new(ms(100), ms(200)).unwrap();
        assert_eq!(r.duration_ms(), 100);
        assert!(!r.is_empty());
        assert!(r.contains_timestamp(ms(150)));
        assert!(!r.contains_timestamp(ms(50)));
        assert!(!r.contains_timestamp(ms(200))); // exclusive end
    }

    #[test]
    fn test_invalid_range() {
        assert!(MarketDataRange::new(ms(200), ms(100)).is_none());
        assert!(MarketDataRange::new(ms(100), ms(100)).is_none());
    }

    #[test]
    fn test_contains() {
        let outer = MarketDataRange::new(ms(100), ms(300)).unwrap();
        let inner = MarketDataRange::new(ms(150), ms(250)).unwrap();
        let partial = MarketDataRange::new(ms(200), ms(400)).unwrap();

        assert!(outer.contains(&inner));
        assert!(!inner.contains(&outer));
        assert!(!outer.contains(&partial));
    }

    #[test]
    fn test_overlaps() {
        let a = MarketDataRange::new(ms(100), ms(200)).unwrap();
        let b = MarketDataRange::new(ms(150), ms(250)).unwrap();
        let c = MarketDataRange::new(ms(300), ms(400)).unwrap();

        assert!(a.overlaps(&b));
        assert!(b.overlaps(&a));
        assert!(!a.overlaps(&c));
    }

    #[test]
    fn test_intersection() {
        let a = MarketDataRange::new(ms(100), ms(200)).unwrap();
        let b = MarketDataRange::new(ms(150), ms(250)).unwrap();
        let c = MarketDataRange::new(ms(300), ms(400)).unwrap();

        let intersection = a.intersection(&b).unwrap();
        assert_eq!(intersection.from, ms(150));
        assert_eq!(intersection.to, ms(200));

        assert!(a.intersection(&c).is_none());
    }

    #[test]
    fn test_merge() {
        let a = MarketDataRange::new(ms(100), ms(200)).unwrap();
        let b = MarketDataRange::new(ms(150), ms(250)).unwrap();
        let c = MarketDataRange::new(ms(300), ms(400)).unwrap();

        let merged = a.merge(&b).unwrap();
        assert_eq!(merged.from, ms(100));
        assert_eq!(merged.to, ms(250));

        // Non-overlapping, non-adjacent
        assert!(a.merge(&c).is_none());
    }

    #[test]
    fn test_adjacent_merge() {
        let a = MarketDataRange::new(ms(100), ms(200)).unwrap();
        let b = MarketDataRange::new(ms(200), ms(300)).unwrap();

        let merged = a.merge(&b).unwrap();
        assert_eq!(merged.from, ms(100));
        assert_eq!(merged.to, ms(300));
    }

    #[test]
    fn test_subtract() {
        let base = MarketDataRange::new(ms(100), ms(300)).unwrap();

        // Subtract middle portion
        let middle = MarketDataRange::new(ms(150), ms(250)).unwrap();
        let result = base.subtract(&middle);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].from, ms(100));
        assert_eq!(result[0].to, ms(150));
        assert_eq!(result[1].from, ms(250));
        assert_eq!(result[1].to, ms(300));

        // Subtract fully contained
        let full = MarketDataRange::new(ms(100), ms(300)).unwrap();
        let result = base.subtract(&full);
        assert!(result.is_empty());

        // Subtract no overlap
        let no_overlap = MarketDataRange::new(ms(400), ms(500)).unwrap();
        let result = base.subtract(&no_overlap);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], base);
    }

    #[test]
    fn test_gap_to() {
        let a = MarketDataRange::new(ms(100), ms(200)).unwrap();
        let b = MarketDataRange::new(ms(300), ms(400)).unwrap();
        let c = MarketDataRange::new(ms(150), ms(250)).unwrap();

        let gap = a.gap_to(&b).unwrap();
        assert_eq!(gap.from, ms(200));
        assert_eq!(gap.to, ms(300));

        // Overlapping - no gap
        assert!(a.gap_to(&c).is_none());
    }

    #[test]
    fn test_split_at() {
        let r = MarketDataRange::new(ms(100), ms(300)).unwrap();

        let (left, right) = r.split_at(ms(200));
        assert_eq!(left.unwrap().to, ms(200));
        assert_eq!(right.unwrap().from, ms(200));

        // Split before start
        let (left, right) = r.split_at(ms(50));
        assert!(left.is_none());
        assert_eq!(right.unwrap(), r);

        // Split after end
        let (left, right) = r.split_at(ms(400));
        assert_eq!(left.unwrap(), r);
        assert!(right.is_none());
    }

    #[test]
    fn test_merge_ranges() {
        let ranges = vec![
            MarketDataRange::new(ms(100), ms(200)).unwrap(),
            MarketDataRange::new(ms(150), ms(250)).unwrap(),
            MarketDataRange::new(ms(300), ms(400)).unwrap(),
            MarketDataRange::new(ms(350), ms(450)).unwrap(),
        ];

        let merged = merge_ranges(&ranges);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].from, ms(100));
        assert_eq!(merged[0].to, ms(250));
        assert_eq!(merged[1].from, ms(300));
        assert_eq!(merged[1].to, ms(450));
    }

    #[test]
    fn test_compute_missing() {
        let desired = MarketDataRange::new(ms(100), ms(500)).unwrap();
        let covered = vec![
            MarketDataRange::new(ms(200), ms(300)).unwrap(),
            MarketDataRange::new(ms(400), ms(450)).unwrap(),
        ];

        let missing = compute_missing(desired, &covered);
        assert_eq!(missing.len(), 3);
        assert_eq!(missing[0].from, ms(100));
        assert_eq!(missing[0].to, ms(200));
        assert_eq!(missing[1].from, ms(300));
        assert_eq!(missing[1].to, ms(400));
        assert_eq!(missing[2].from, ms(450));
        assert_eq!(missing[2].to, ms(500));
    }

    #[test]
    fn test_is_fully_covered() {
        let range = MarketDataRange::new(ms(100), ms(300)).unwrap();

        let full_cover = vec![
            MarketDataRange::new(ms(100), ms(200)).unwrap(),
            MarketDataRange::new(ms(200), ms(300)).unwrap(),
        ];
        assert!(is_fully_covered(&range, &full_cover));

        let partial_cover = vec![MarketDataRange::new(ms(100), ms(200)).unwrap()];
        assert!(!is_fully_covered(&range, &partial_cover));

        let empty_cover = vec![];
        assert!(!is_fully_covered(&range, &empty_cover));
    }

    #[test]
    fn test_add_required_segment_dedup_skips_fully_covered() {
        let mut required = vec![MarketDataRange::new(ms(100), ms(300)).unwrap()];
        // Sub-segment is fully covered by the broad segment - should be skipped
        add_required_segment_dedup(
            &mut required,
            MarketDataRange::new(ms(100), ms(200)).unwrap(),
        );
        assert_eq!(required.len(), 1);
        assert_eq!(required[0], MarketDataRange::new(ms(100), ms(300)).unwrap());
    }

    #[test]
    fn test_add_required_segment_dedup_adds_uncovered() {
        let mut required = vec![MarketDataRange::new(ms(100), ms(200)).unwrap()];
        // Adjacent segment is not covered - should be added
        add_required_segment_dedup(
            &mut required,
            MarketDataRange::new(ms(200), ms(300)).unwrap(),
        );
        assert_eq!(required.len(), 2);
        assert_eq!(required[0], MarketDataRange::new(ms(100), ms(200)).unwrap());
        assert_eq!(required[1], MarketDataRange::new(ms(200), ms(300)).unwrap());
    }

    #[test]
    fn test_add_required_segment_dedup_does_not_replace_adjacent_segments() {
        // When two adjacent segments [100,200) and [200,300) already exist,
        // adding [100,300) should be skipped because it's fully covered.
        // Adjacent segments must remain separate for logical counting.
        let mut required = vec![
            MarketDataRange::new(ms(100), ms(200)).unwrap(),
            MarketDataRange::new(ms(200), ms(300)).unwrap(),
        ];
        add_required_segment_dedup(
            &mut required,
            MarketDataRange::new(ms(100), ms(300)).unwrap(),
        );
        assert_eq!(required.len(), 2);
        assert_eq!(required[0], MarketDataRange::new(ms(100), ms(200)).unwrap());
        assert_eq!(required[1], MarketDataRange::new(ms(200), ms(300)).unwrap());
    }

    #[test]
    fn test_filter_tiny_trade_gaps() {
        let missing = vec![
            MarketDataRange::new(ms(100), ms(1200)).unwrap(), // 1100ms - not tiny
            MarketDataRange::new(ms(3000), ms(3001)).unwrap(), // 1ms - tiny
            MarketDataRange::new(ms(4000), ms(6000)).unwrap(), // 2000ms - not tiny
        ];
        let filtered = filter_tiny_trade_gaps(missing, 1_000);
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].from, ms(100));
        assert_eq!(filtered[0].to, ms(1200));
        assert_eq!(filtered[1].from, ms(4000));
        assert_eq!(filtered[1].to, ms(6000));
    }

    #[test]
    fn test_filter_tiny_trade_gaps_all_tiny() {
        let missing = vec![
            MarketDataRange::new(ms(100), ms(101)).unwrap(), // 1ms
            MarketDataRange::new(ms(200), ms(250)).unwrap(), // 50ms
        ];
        let filtered = filter_tiny_trade_gaps(missing, 1_000);
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_filter_tiny_trade_gaps_none_tiny() {
        let missing = vec![
            MarketDataRange::new(ms(100), ms(1200)).unwrap(), // 1100ms
            MarketDataRange::new(ms(2000), ms(5000)).unwrap(), // 3000ms
        ];
        let filtered = filter_tiny_trade_gaps(missing, 1_000);
        assert_eq!(filtered.len(), 2);
    }
}
