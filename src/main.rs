use clap::{ArgAction, Parser, ValueEnum};
use console::{set_colors_enabled, style};
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::time::{Duration, Instant};

#[derive(ValueEnum, Clone, Debug)]
enum LogFormat {
    Default,
    Csv,
}

#[derive(Parser, Debug)]
#[clap(author, about, version)]
#[command(name = "dump-filter")]
struct Cli {
    /// The path to the mysqldump file
    #[arg()]
    file: String,

    /// Specify tables to exclude INSERT statements for (csv allowed)
    #[arg(short, long, action = ArgAction::Append, value_name = "TABLE", visible_aliases = ["ignore", "exclude"])]
    except: Vec<String>,

    /// Log time spent on each CREATE TABLE and INSERT INTO statement
    #[arg(long, action = ArgAction::SetTrue)]
    log: bool,

    /// Format of the log output (default or csv)
    #[arg(long, default_value = "default", value_enum)]
    format: LogFormat,

    /// Show a progress bar
    #[arg(long, action = ArgAction::SetTrue)]
    progress: bool,
}

// State enum to clearly manage timing and transitions
enum State {
    None,
    CreateTable {
        table_name: String,
        start_time: Instant,
    },
    InsertInto {
        table_name: String,
        start_time: Instant,
    },
}

fn main() -> io::Result<()> {
    let cli = Cli::parse();
    let excluded_tables: Vec<String> = cli
        .except
        .iter()
        .flat_map(|s| s.split(','))
        .map(|s| s.trim().to_string())
        .collect();

    stream_dump(
        &cli.file,
        &excluded_tables,
        cli.log,
        &cli.format,
        cli.progress,
    )
}

fn stream_dump(
    file_path: &str,
    excluded_tables: &[String],
    log: bool,
    format: &LogFormat,
    show_progress: bool,
) -> io::Result<()> {
    let file = File::open(file_path)?;
    let metadata = file.metadata()?;
    let file_size = metadata.len();

    let stdout = io::stdout();
    let mut writer = stdout.lock();

    // Conditionally create the progress bar
    let progress_bar = if show_progress {
        let pb = ProgressBar::new(file_size);
        let progress_style =
            ProgressStyle::with_template("{bar:40} {bytes}/{total_bytes} ({eta}) {wide_msg}")
                .unwrap();
        pb.set_style(progress_style);
        set_colors_enabled(true);
        pb.enable_steady_tick(Duration::from_millis(100));
        Some(pb)
    } else {
        None
    };

    let mut state = State::None;
    let mut skip_inserts = false;
    let mut buffer = Vec::new();
    let mut reader = BufReader::new(file);

    while reader.read_until(b'\n', &mut buffer)? > 0 {
        if let Some(pb) = &progress_bar {
            pb.inc(buffer.len() as u64);
        }
        let line = String::from_utf8_lossy(&buffer);

        // Handle CREATE TABLE
        if line.starts_with("CREATE TABLE") {
            if let State::InsertInto {
                table_name,
                start_time,
            } = &state
            {
                if log {
                    if let Some(pb) = &progress_bar {
                        pb.suspend(|| {
                            log_time(format, "INSERT INTO", table_name, start_time.elapsed())
                        });
                    } else {
                        log_time(format, "INSERT INTO", table_name, start_time.elapsed());
                    }
                }
            } else if let State::CreateTable {
                table_name,
                start_time,
            } = &state
            {
                if log {
                    if let Some(pb) = &progress_bar {
                        pb.suspend(|| {
                            log_time(format, "CREATE TABLE", table_name, start_time.elapsed())
                        });
                    } else {
                        log_time(format, "CREATE TABLE", table_name, start_time.elapsed());
                    }
                }
            }

            if let Some(table_name) = extract_table_name(&line) {
                let colored_table_name = if excluded_tables.contains(&table_name) {
                    format!("{} (skip)", style(&table_name).black().bright())
                } else {
                    format!("{}", style(&table_name).green())
                };
                if let Some(pb) = &progress_bar {
                    pb.set_message(format!("Table: {}", colored_table_name));
                }
                skip_inserts = excluded_tables.contains(&table_name);

                state = State::CreateTable {
                    table_name,
                    start_time: Instant::now(),
                };
            }
        }
        // Handle INSERT INTO
        else if line.starts_with("INSERT INTO") {
            if let State::CreateTable {
                table_name,
                start_time,
            } = &state
            {
                if log {
                    if let Some(pb) = &progress_bar {
                        pb.suspend(|| {
                            log_time(format, "CREATE TABLE", table_name, start_time.elapsed())
                        });
                    } else {
                        log_time(format, "CREATE TABLE", table_name, start_time.elapsed());
                    }
                }
                if !skip_inserts {
                    state = State::InsertInto {
                        table_name: table_name.clone(),
                        start_time: Instant::now(),
                    };
                }
            }
        }
        // Handle UNLOCK TABLES
        else if line.starts_with("UNLOCK TABLES;") {
            if let State::InsertInto {
                table_name,
                start_time,
            } = &state
            {
                if log {
                    if let Some(pb) = &progress_bar {
                        pb.suspend(|| {
                            log_time(format, "INSERT INTO", table_name, start_time.elapsed())
                        });
                    } else {
                        log_time(format, "INSERT INTO", table_name, start_time.elapsed());
                    }
                }
            }
            state = State::None;
            if let Some(pb) = &progress_bar {
                pb.set_message("Processing...");
            }
        }

        // Output the line if not skipping inserts
        if !skip_inserts || !line.starts_with("INSERT INTO") {
            writer.write_all(&buffer)?;
        }

        buffer.clear(); // Clear the buffer for the next line
    }

    // Final measurement at EOF
    if let State::CreateTable {
        ref table_name,
        ref start_time,
    }
    | State::InsertInto {
        ref table_name,
        ref start_time,
    } = state
    {
        if log {
            let statement_type = match state {
                State::CreateTable { .. } => "CREATE TABLE",
                State::InsertInto { .. } => "INSERT INTO",
                _ => "Unknown",
            };
            if let Some(pb) = &progress_bar {
                pb.suspend(|| log_time(format, statement_type, table_name, start_time.elapsed()));
            } else {
                log_time(format, statement_type, table_name, start_time.elapsed());
            }
        }
    }

    if let Some(pb) = progress_bar {
        pb.finish_and_clear();
    }
    Ok(())
}

fn log_time(format: &LogFormat, statement: &str, table_name: &str, duration: Duration) {
    match format {
        LogFormat::Default => {
            eprintln!(
                "{} {} took {} ms",
                statement,
                table_name,
                duration.as_millis()
            );
        }
        LogFormat::Csv => {
            let statement_type = if statement == "CREATE TABLE" {
                "CREATE"
            } else {
                "INSERT"
            };
            eprintln!("{},{},{}", statement_type, table_name, duration.as_millis());
        }
    }
}

fn extract_table_name(line: &str) -> Option<String> {
    line.split_whitespace()
        .nth(2)
        .map(|name| name.trim_matches('`').to_string())
}
