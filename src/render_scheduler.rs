//! Render/dirty tracking for efficient frame rendering.
//!
//! Instead of forcing redraw every 16ms for all windows/panes,
//! mark the UI dirty when market data arrives and render only when needed.

use std::sync::atomic::{AtomicBool, Ordering};

/// Global dirty flag for the UI.
///
/// Set to `true` when market data arrives or user interaction changes state.
/// The UI framework (iced) handles the actual render scheduling.
#[derive(Debug)]
pub struct DirtyFlag {
    dirty: AtomicBool,
}

impl DirtyFlag {
    pub fn new() -> Self {
        Self {
            dirty: AtomicBool::new(true),
        }
    }

    /// Mark the UI as needing a re-render.
    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }
}

impl Default for DirtyFlag {
    fn default() -> Self {
        Self::new()
    }
}
