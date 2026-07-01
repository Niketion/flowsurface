use std::{
    backtrace::Backtrace,
    collections::VecDeque,
    fs,
    io::{self, Write},
    panic::PanicHookInfo,
    path::PathBuf,
    process,
    sync::{Mutex, Once, mpsc},
    thread::{self, JoinHandle},
};

const MAX_LOG_FILE_SIZE: u64 = 50 * 1024 * 1024; // 50 MB
const MAX_DEBUG_LINES: usize = 2_000;

static DEBUG_LOGS: Mutex<VecDeque<String>> = Mutex::new(VecDeque::new());
static DEBUG_PENDING_LINE: Mutex<String> = Mutex::new(String::new());

enum LogMessage {
    Content(Vec<u8>),
    Flush,
    Shutdown,
}

pub fn setup(is_debug: bool) -> Result<(), data::log::Error> {
    let default_level = if is_debug {
        log::Level::Debug
    } else {
        log::Level::Info
    };

    let level_filter = std::env::var("RUST_LOG")
        .ok()
        .as_deref()
        .map(str::parse::<log::Level>)
        .transpose()?
        .unwrap_or(default_level)
        .to_level_filter();

    let mut io_sink = fern::Dispatch::new();

    if is_debug {
        io_sink = io_sink.chain(std::io::stdout());
    } else {
        let log_path = data::log::path()?;
        initial_rotation(&log_path)?;

        let logger: Box<dyn Write + Send> = Box::new(BackgroundLogger::new(log_path)?);

        io_sink = io_sink.chain(logger);
    }

    fern::Dispatch::new()
        .format(|out, message, record| {
            let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
            let prefix = format!(
                "[{}] [{:<5}] [{}]",
                timestamp,
                record.level(),
                record.target()
            );
            let message = message.to_string();
            let formatted = message
                .lines()
                .map(|line| format!("{prefix} {line}"))
                .collect::<Vec<_>>()
                .join("\n");

            out.finish(format_args!("{formatted}"));
        })
        .level(log::LevelFilter::Off)
        .level_for("panic", log::LevelFilter::Error)
        // Silence noisy third-party crates by default; override with RUST_LOG.
        .level_for("iced_wgpu", log::LevelFilter::Warn)
        .level_for("wgpu", log::LevelFilter::Warn)
        .level_for("naga", log::LevelFilter::Warn)
        .level_for("wgpu_core", log::LevelFilter::Warn)
        .level_for("wgpu_hal", log::LevelFilter::Warn)
        .level_for("iced_winit", log::LevelFilter::Warn)
        .level_for("iced_graphics", log::LevelFilter::Warn)
        .level_for("flowsurface_exchange", level_filter)
        .level_for("flowsurface_data", level_filter)
        .level_for("flowsurface", level_filter)
        .chain(io_sink)
        .chain(Box::new(DebugTerminalLogger) as Box<dyn Write + Send>)
        .apply()?;

    Ok(())
}

pub fn debug_terminal_snapshot() -> Vec<String> {
    DEBUG_LOGS
        .lock()
        .map(|lines| lines.iter().cloned().collect())
        .unwrap_or_default()
}

pub fn clear_debug_terminal() {
    if let Ok(mut lines) = DEBUG_LOGS.lock() {
        lines.clear();
    }

    if let Ok(mut pending) = DEBUG_PENDING_LINE.lock() {
        pending.clear();
    }
}

fn push_debug_line(line: String) {
    if line.trim().is_empty() {
        return;
    }

    if let Ok(mut lines) = DEBUG_LOGS.lock() {
        if lines.len() >= MAX_DEBUG_LINES {
            lines.pop_front();
        }

        lines.push_back(line);
    }
}

struct DebugTerminalLogger;

impl Write for DebugTerminalLogger {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = buf.len();
        let content = String::from_utf8_lossy(buf);
        let mut completed = Vec::new();

        if let Ok(mut pending) = DEBUG_PENDING_LINE.lock() {
            pending.push_str(&content);

            while let Some(newline_index) = pending.find('\n') {
                let mut line = pending.drain(..=newline_index).collect::<String>();
                if line.ends_with('\n') {
                    line.pop();
                }
                if line.ends_with('\r') {
                    line.pop();
                }

                completed.push(line);
            }
        }

        for line in completed {
            push_debug_line(line);
        }

        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        let pending = DEBUG_PENDING_LINE.lock().ok().and_then(|mut pending| {
            if pending.trim().is_empty() {
                pending.clear();
                None
            } else {
                Some(std::mem::take(&mut *pending))
            }
        });

        if let Some(line) = pending {
            push_debug_line(line);
        }

        Ok(())
    }
}

fn initial_rotation(log_path: &PathBuf) -> io::Result<()> {
    let dir = log_path.parent().unwrap_or(std::path::Path::new("."));
    let previous = dir.join("flowsurface-previous.log");

    if let Err(e) = fs::remove_file(&previous)
        && e.kind() != io::ErrorKind::NotFound
    {
        return Err(e);
    }
    if let Err(e) = fs::rename(log_path, &previous)
        && e.kind() != io::ErrorKind::NotFound
    {
        return Err(e);
    }
    Ok(())
}

struct BackgroundLogger {
    sender: mpsc::Sender<LogMessage>,
    thread_handle: Option<JoinHandle<()>>,
}

impl BackgroundLogger {
    fn new(path: PathBuf) -> io::Result<Self> {
        let (sender, receiver) = mpsc::channel();

        let thread_handle = thread::Builder::new()
            .name("logger-thread".to_string())
            .spawn(move || {
                let mut logger = match Logger::new(&path) {
                    Ok(logger) => logger,
                    Err(e) => {
                        eprintln!("Failed to initialize logger: {}", e);
                        return;
                    }
                };

                loop {
                    match receiver.recv() {
                        Ok(LogMessage::Content(data)) => {
                            if let Err(e) = logger.write_all(&data) {
                                eprintln!("Logging error: {}", e);
                            }
                        }
                        Ok(LogMessage::Flush) => {
                            if let Err(e) = logger.flush() {
                                eprintln!("Error flushing logs: {}", e);
                            }
                        }
                        Ok(LogMessage::Shutdown) | Err(_) => break,
                    }
                }
            })?;

        Ok(BackgroundLogger {
            sender,
            thread_handle: Some(thread_handle),
        })
    }
}

impl Write for BackgroundLogger {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = buf.len();
        self.sender
            .send(LogMessage::Content(buf.to_vec()))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Logger thread disconnected"))?;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.sender
            .send(LogMessage::Flush)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Logger thread disconnected"))?;
        Ok(())
    }
}

impl Drop for BackgroundLogger {
    fn drop(&mut self) {
        let _ = self.sender.send(LogMessage::Shutdown);
        if let Some(handle) = self.thread_handle.take()
            && let Err(err) = handle.join()
        {
            eprintln!("Background logger thread panicked: {err:?}");
        }
    }
}

struct Logger {
    file: fs::File,
    current_size: u64,
}

impl Logger {
    fn new(path: &PathBuf) -> io::Result<Self> {
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        let size = file.metadata()?.len();

        Ok(Logger {
            file,
            current_size: size,
        })
    }
}

impl Write for Logger {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let buf_len = buf.len() as u64;

        if self.current_size + buf_len > MAX_LOG_FILE_SIZE {
            let timestamp = chrono::Local::now().format("%H:%M:%S%.3f");
            let error_msg = format!(
                "\n{}:FATAL -- Log file size would exceed the maximum allowed size of {} bytes\n",
                timestamp, MAX_LOG_FILE_SIZE
            );

            eprintln!("{error_msg}");

            let _ = self.file.write_all(error_msg.as_bytes());
            let _ = self.file.flush();

            process::abort();
        }

        let bytes = self.file.write(buf)?;
        self.current_size += bytes as u64;

        Ok(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

pub fn install_panic_hook() {
    static PANIC_HOOK: Once = Once::new();

    PANIC_HOOK.call_once(|| {
        let previous = std::panic::take_hook();

        std::panic::set_hook(Box::new(move |panic_info| {
            let report = format_panic_report(panic_info);

            log::error!(target: "panic", "{report}");
            log::logger().flush();

            if let Err(err) = append_stderr_log_line(&report) {
                eprintln!("Failed to persist panic report: {err}");
            }

            previous(panic_info);
        }));
    });
}

pub fn report_stderr(message: &str) {
    if let Err(err) = append_stderr_log_line(message) {
        eprintln!("Failed to persist std log entry: {err}");
    }

    eprintln!("{message}");
}

fn format_panic_report(info: &PanicHookInfo<'_>) -> String {
    let current_thread = thread::current();
    let thread_name = current_thread.name().unwrap_or("unnamed");
    let location = info
        .location()
        .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
        .unwrap_or_else(|| "unknown location".to_string());

    let payload = info
        .payload()
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| info.payload().downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string());

    let backtrace = Backtrace::force_capture();

    format!("panic in thread '{thread_name}' at {location}: {payload}\nBacktrace:\n{backtrace}")
}

fn append_stderr_log_line(message: &str) -> io::Result<()> {
    let log_path = data::log::path().map_err(|err| io::Error::other(err.to_string()))?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;

    writeln!(
        file,
        "{}:FATAL -- {message}",
        chrono::Local::now().format("%H:%M:%S%.3f"),
    )?;

    file.flush()
}
