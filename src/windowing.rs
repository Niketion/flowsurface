//! Windowing mode abstraction for platform-specific behavior.
//!
//! On Windows, winit multi-window redraw (issue #3648/#4460) causes
//! `RedrawRequested` starvation when multiple native windows request
//! redraws simultaneously. The workaround is to use a single native
//! window with internal overlays/docked panels instead of native popouts.
//!
//! On macOS/Linux, native multi-window works correctly.

/// Determines how the application handles multiple windows and popouts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowingMode {
    /// Each pane popout opens a separate OS-native window.
    /// Works correctly on macOS and Linux.
    NativeMultiWindow,
    /// All UI is rendered inside a single native window.
    /// Panes that would pop out are instead docked/maximized internally.
    /// Required on Windows due to winit multi-window redraw bugs.
    SingleWindowEmbedded,
}

impl WindowingMode {
    /// Returns the default windowing mode for the current platform.
    ///
    /// - Windows: `SingleWindowEmbedded` (winit #3648/#4460 workaround)
    /// - macOS/Linux: `NativeMultiWindow`
    pub fn platform_default() -> Self {
        if cfg!(target_os = "windows") {
            Self::SingleWindowEmbedded
        } else {
            Self::NativeMultiWindow
        }
    }

    /// Returns `true` if native popout windows are allowed.
    pub fn allows_native_popout(&self) -> bool {
        matches!(self, Self::NativeMultiWindow)
    }

    /// Returns a human-readable reason string for logging.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::NativeMultiWindow => "platform_supported",
            Self::SingleWindowEmbedded => "winit_win32_redraw_bug",
        }
    }
}

impl std::fmt::Display for WindowingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NativeMultiWindow => write!(f, "NativeMultiWindow"),
            Self::SingleWindowEmbedded => write!(f, "SingleWindowEmbedded"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_default_is_correct() {
        #[cfg(target_os = "windows")]
        assert_eq!(
            WindowingMode::platform_default(),
            WindowingMode::SingleWindowEmbedded
        );

        #[cfg(not(target_os = "windows"))]
        assert_eq!(
            WindowingMode::platform_default(),
            WindowingMode::NativeMultiWindow
        );
    }

    #[test]
    fn native_popout_only_in_multi_window() {
        assert!(WindowingMode::NativeMultiWindow.allows_native_popout());
        assert!(!WindowingMode::SingleWindowEmbedded.allows_native_popout());
    }
}
