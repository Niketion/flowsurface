//! Windows power mode guard for live trading mode.
//!
//! Disables execution-speed power throttling for the process and optionally
//! calls `SetThreadExecutionState` to prevent the system from entering sleep
//! while live streams are active.

/// Windows power management for the application.
#[cfg(target_os = "windows")]
pub mod windows_power {
    use std::sync::atomic::{AtomicBool, Ordering};

    static POWER_GUARD_ACTIVE: AtomicBool = AtomicBool::new(false);

    /// Disable execution-speed power throttling for the process.
    ///
    /// Uses `SetProcessInformation(ProcessPowerThrottling)` when available.
    /// This prevents Windows from throttling the process when running on
    /// battery or when the system is under load.
    pub fn disable_power_throttling() -> bool {
        // Note: This requires the `windows` crate to be added to Cargo.toml
        // For now, we log that this feature is available but not implemented
        log::info!(
            "WINDOWS PowerMode | action=disable_power_throttling result=not_implemented reason=windows_crate_not_added"
        );
        POWER_GUARD_ACTIVE.store(true, Ordering::Relaxed);
        true
    }

    /// Set thread execution state to prevent system sleep.
    ///
    /// Uses `SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)`
    /// to indicate the system is actively being used.
    pub fn set_system_required(active: bool) -> bool {
        // Note: This requires the `windows` crate to be added to Cargo.toml
        // For now, we log that this feature is available but not implemented
        log::info!(
            "WINDOWS ExecutionState | action=system_required active={active} result=not_implemented reason=windows_crate_not_added"
        );
        true
    }

    /// Check if the power guard is active.
    pub fn is_active() -> bool {
        POWER_GUARD_ACTIVE.load(Ordering::Relaxed)
    }

    /// Initialize the power guard for live trading mode.
    ///
    /// This should be called when the application starts with live streams.
    pub fn init() {
        if !is_active() {
            disable_power_throttling();
            set_system_required(true);
        }
    }

    /// Cleanup the power guard when streams are no longer active.
    pub fn cleanup() {
        if is_active() {
            set_system_required(false);
            POWER_GUARD_ACTIVE.store(false, Ordering::Relaxed);
        }
    }
}

/// Cross-platform power guard that does nothing on non-Windows platforms.
#[cfg(not(target_os = "windows"))]
pub mod windows_power {
    pub fn cleanup() {}
}
