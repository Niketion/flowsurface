//! Render/dirty scheduler for efficient frame rendering.
//!
//! Instead of forcing redraw every 16ms for all windows/panes,
//! this scheduler marks panes dirty when market data or user interaction
//! changes them, and renders active/visible panes at high FPS while
//! rendering inactive/background panes at lower FPS or not at all.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Tracks the render state of a single pane.
#[derive(Debug)]
pub struct PaneRenderState {
    /// Whether the pane has been marked dirty and needs re-rendering.
    pub dirty: bool,
    /// Last time this pane was rendered.
    pub last_rendered: Option<Instant>,
    /// Whether this pane is currently visible (not occluded/minimized).
    pub visible: bool,
    /// Whether this pane is the focused/active pane.
    pub focused: bool,
    /// The render interval for this pane based on its state.
    pub render_interval: Duration,
}

impl Default for PaneRenderState {
    fn default() -> Self {
        Self {
            dirty: true,
            last_rendered: None,
            visible: true,
            focused: false,
            render_interval: Duration::from_millis(16), // ~60fps default
        }
    }
}

/// Priority levels for rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RenderPriority {
    /// Active pane with user interaction - render at full FPS.
    Active,
    /// Visible pane receiving market data - render at moderate FPS.
    Visible,
    /// Background/minimized pane - render at low FPS or skip.
    Background,
}

/// The dirty scheduler manages which panes need rendering.
#[derive(Debug)]
pub struct DirtyScheduler {
    /// Per-pane render states, keyed by pane UUID.
    pane_states: HashMap<uuid::Uuid, PaneRenderState>,
    /// Active FPS for focused panes.
    active_fps: u32,
    /// Visible FPS for visible but unfocused panes.
    visible_fps: u32,
    /// Background FPS for background/minimized panes.
    background_fps: u32,
}

impl DirtyScheduler {
    pub fn new() -> Self {
        Self {
            pane_states: HashMap::new(),
            active_fps: 60,
            visible_fps: 30,
            background_fps: 5,
        }
    }

    /// Register a pane with the scheduler.
    pub fn register_pane(&mut self, pane_id: uuid::Uuid) {
        self.pane_states.entry(pane_id).or_default();
    }

    /// Remove a pane from the scheduler.
    pub fn unregister_pane(&mut self, pane_id: &uuid::Uuid) {
        self.pane_states.remove(pane_id);
    }

    /// Mark a pane as dirty (needs re-rendering).
    pub fn mark_dirty(&mut self, pane_id: &uuid::Uuid) {
        if let Some(state) = self.pane_states.get_mut(pane_id) {
            state.dirty = true;
        }
    }

    /// Mark all panes as dirty.
    pub fn mark_all_dirty(&mut self) {
        for state in self.pane_states.values_mut() {
            state.dirty = true;
        }
    }

    /// Set whether a pane is visible.
    pub fn set_visible(&mut self, pane_id: &uuid::Uuid, visible: bool) {
        if let Some(state) = self.pane_states.get_mut(pane_id) {
            state.visible = visible;
            let interval = compute_interval(
                state.focused,
                state.visible,
                self.active_fps,
                self.visible_fps,
                self.background_fps,
            );
            state.render_interval = interval;
        }
    }

    /// Set whether a pane is focused.
    pub fn set_focused(&mut self, pane_id: &uuid::Uuid, focused: bool) {
        if let Some(state) = self.pane_states.get_mut(pane_id) {
            state.focused = focused;
            let interval = compute_interval(
                state.focused,
                state.visible,
                self.active_fps,
                self.visible_fps,
                self.background_fps,
            );
            state.render_interval = interval;
        }
    }

    /// Get the render priority for a pane.
    pub fn priority(&self, pane_id: &uuid::Uuid) -> RenderPriority {
        match self.pane_states.get(pane_id) {
            Some(state) if state.focused => RenderPriority::Active,
            Some(state) if state.visible => RenderPriority::Visible,
            _ => RenderPriority::Background,
        }
    }

    /// Check if a pane should be rendered now.
    pub fn should_render(&self, pane_id: &uuid::Uuid, now: Instant) -> bool {
        let Some(state) = self.pane_states.get(pane_id) else {
            return false;
        };

        if !state.dirty {
            return false;
        }

        // If never rendered, always render
        let Some(last) = state.last_rendered else {
            return true;
        };

        // Check if enough time has elapsed since last render
        now.duration_since(last) >= state.render_interval
    }

    /// Get all panes that should be rendered now.
    pub fn panes_to_render(&self, now: Instant) -> Vec<uuid::Uuid> {
        self.pane_states
            .iter()
            .filter(|(id, _)| self.should_render(id, now))
            .map(|(id, _)| *id)
            .collect()
    }

    /// Mark a pane as rendered (clears dirty flag).
    pub fn mark_rendered(&mut self, pane_id: &uuid::Uuid, now: Instant) {
        if let Some(state) = self.pane_states.get_mut(pane_id) {
            state.dirty = false;
            state.last_rendered = Some(now);
        }
    }

    /// Get statistics about the scheduler state.
    pub fn stats(&self) -> SchedulerStats {
        let total = self.pane_states.len();
        let dirty = self.pane_states.values().filter(|s| s.dirty).count();
        let visible = self.pane_states.values().filter(|s| s.visible).count();
        let focused = self.pane_states.values().filter(|s| s.focused).count();

        SchedulerStats {
            total_panes: total,
            dirty_panes: dirty,
            visible_panes: visible,
            focused_panes: focused,
        }
    }
}

/// Compute render interval based on pane state (extracted to avoid borrow issues).
fn compute_interval(
    focused: bool,
    visible: bool,
    active_fps: u32,
    visible_fps: u32,
    background_fps: u32,
) -> Duration {
    if focused {
        Duration::from_secs(1) / active_fps
    } else if visible {
        Duration::from_secs(1) / visible_fps
    } else {
        Duration::from_secs(1) / background_fps
    }
}

impl Default for DirtyScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the scheduler state.
#[derive(Debug)]
pub struct SchedulerStats {
    pub total_panes: usize,
    pub dirty_panes: usize,
    pub visible_panes: usize,
    pub focused_panes: usize,
}

impl std::fmt::Display for SchedulerStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "panes={} dirty={} visible={} focused={}",
            self.total_panes, self.dirty_panes, self.visible_panes, self.focused_panes
        )
    }
}
