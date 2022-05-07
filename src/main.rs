/* Copyright 2022 Ivan Boldyrev
 *
 * Licensed under the MIT License.
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

use anyhow::{self as ah, Context as _};
use chrono::TimeZone;
use clap::Parser;
use interruptable::Interruptable;
use std::{
    ffi::OsString,
    io::{self, Write as _},
    ops::Deref,
    path::{Path, PathBuf},
    process::{exit, Child, Command, Stdio},
    str::FromStr,
    sync::{atomic::AtomicBool, Arc},
};

const TAR_HEADER_SIZE: u64 = 512;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("file {:?} with its header is larger than --max-size", .0)]
    FileTooLarge(String),
    #[error(transparent)]
    Other(#[from] ah::Error),
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        Self::Other(source.into())
    }
}

type Result<T> = std::result::Result<T, Error>;

// Simple wrapper for binary one-letter units (like 300G).
fn clap_parse_size(src: &str) -> std::result::Result<u64, parse_size::Error> {
    parse_size::Config::new().with_binary().parse_size(src)
}

#[derive(Parser, Debug)]
#[clap(about, version, author)]
struct Args {
    #[clap(
        short = 'S',
        long,
        parse(try_from_str = clap_parse_size),
        help = "max data size per output volume",
    )]
    max_size: u64,
    #[clap(long, help = "fail if a file is too large to fit into single volume")]
    fail_on_large_file: bool,
    #[clap(
        short = 'v',
        long,
        help = "output files info prefixed with volume number"
    )]
    verbose: bool,
    #[clap(short = 'd', long, help = "recreate dirs in new volumes")]
    recreate_dirs: bool,
    #[clap(long)]
    compress: Option<String>,
    #[clap(short = 'a', long, default_value = "5")]
    suffix_length: u8,
    #[clap(help = "input file path or `-` for stdin")]
    input_file: PathBuf,
    output_prefix: String,
}

type SplitarRead = Interruptable<io::BufWriter<Box<dyn io::Write>>, Arc<AtomicBool>>;

// This struct has some Option<T> field.  They are always
// Some(_), except Drop::drop or similar methods.
struct Volume {
    acc_size: u64,
    builder: Option<tar::Builder<SplitarRead>>,
    temp_output: Option<tempfile::TempPath>,
    target_file: PathBuf,
    subprocess: Option<Child>,
    prev_dir: Vec<u8>,
    stored_dirs: patricia_tree::PatriciaSet,
    volume_name: String,
}

impl Volume {
    fn new(vol_idx: usize, args: &Args, interrupt_flag: Arc<AtomicBool>) -> ah::Result<Self> {
        let volume_name = format!(
            "{index:0>width$}",
            width = args.suffix_length as _,
            index = vol_idx,
        );
        let target_file = PathBuf::from_str(&format!(
            "{path}{volume}",
            path = args.output_prefix,
            volume = volume_name,
        ))
        .context("internal: failed to contstruct output path")?;
        log::info!("Starting new volume: {:?}", target_file);
        log::debug!("Creating temp file for output");
        let out_temp_file = tempfile::Builder::new()
            // Unwrap is ok as we construct the path with numbers, see above
            .prefix(target_file.file_name().unwrap())
            .rand_bytes(args.suffix_length as _)
            .suffix(".tmp")
            .tempfile_in(target_file.parent().unwrap_or_else(|| Path::new(".")))
            .context("failed to create output tempfile")?;
        let (out_file, temp_output) = out_temp_file.into_parts();
        log::debug!("Output temp file {:?}", temp_output);

        let mut maybe_subprocess = None;

        let out_file = match &args.compress {
            Some(compress) => {
                let shell = std::env::var_os("SHELL").unwrap_or_else(|| {
                    OsString::from_str("/bin/bash").expect("internal: can't run on this os")
                });
                let mut subprocess = Command::new(shell.clone())
                    .arg("-c")
                    .arg(compress)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::from(out_file))
                    .spawn()
                    .with_context(|| {
                        format!("failed to start {:?} with shell {:?}", compress, shell)
                    })?;
                log::info!("Executing subprocess {}", subprocess.id());

                let out = Box::new(
                    subprocess
                        .stdin
                        .take()
                        .expect("internal: expecting subprocess stdin"),
                ) as Box<dyn io::Write>;
                // This supborcess has stdin field empty, but we do not use it anyway.
                maybe_subprocess = Some(subprocess);

                out
            }
            None => Box::new(out_file),
        };

        let builder = tar::Builder::new(Interruptable::new(
            io::BufWriter::with_capacity(
                /* 16384 is default pipe buffer size for Linux;
                 * on MacOS, it can grow on demand up to this value.
                 * We are using half of this value.
                 */
                1 << 13,
                out_file,
            ),
            interrupt_flag,
        ));

        Ok(Self {
            acc_size: 2 * TAR_HEADER_SIZE, // Account two EOF empty headers
            builder: Some(builder),
            temp_output: Some(temp_output),
            target_file,
            subprocess: maybe_subprocess,
            prev_dir: vec![],
            stored_dirs: Default::default(),
            volume_name,
        })
    }

    fn write_data<R: io::Read>(
        &mut self,
        header: &tar::Header,
        data: R,
        verbose: bool,
    ) -> ah::Result<()> {
        if verbose {
            print_header(&self.volume_name, header)
                .context("failed to output verbose file info")?;
        }
        self.builder
            .as_mut()
            .unwrap()
            .append(header, data)
            .context("failed to write an entry to output file")?;
        self.acc_size += header.size()? + TAR_HEADER_SIZE;
        Ok(())
    }

    /// Insert dirs known so far for particular path, unless they was already
    /// inserted into particular volume.
    fn inject_dirs_for_path(
        &mut self,
        dirname: &[u8],
        known_dirs: &patricia_tree::PatriciaMap<Box<tar::Header>>,
        verbose: bool,
    ) -> ah::Result<()> {
        for header in known_dirs.common_prefix_values(dirname) {
            let path_bytes = header.path_bytes();
            if !self.stored_dirs.contains(header.path_bytes()) {
                log::debug!(
                    "Dirname {:?} is new for the volume, inserting...",
                    String::from_utf8_lossy(&path_bytes),
                );
                self.write_data(header, vec![].as_slice(), verbose)?;
                self.stored_dirs.insert(header.path_bytes());
            } else {
                log::debug!(
                    "Dirname {:?} already inserted, skipping...",
                    String::from_utf8_lossy(&path_bytes),
                );
            }
        }
        Ok(())
    }

    /// Complete writing the volume: finish the builder, wait the subprocess
    /// to finish, and rename the temp file to the target file.
    /// If this method is not called, the Drop implementation will rollback
    /// everything.
    fn finish(mut self) -> ah::Result<()> {
        // Finish the builder, and drop it, closing the
        // underlying file.
        self.builder
            .take()
            .unwrap()
            .finish()
            .context("failed to write final data to output file")?;

        // It is important that we call the Builder::finish first
        if let Some(mut subprocess) = self.subprocess.take() {
            log::info!("Waiting subprocess {} to finish", subprocess.id());
            let ret = subprocess
                .wait()
                .context("failed to wait for subprocess completion")?;

            ah::ensure!(
                ret.success(),
                "subprocess exited with error: {}",
                ret.code().unwrap_or(-1)
            );
        }

        log::debug!("Moving {:?} to {:?}", self.temp_output, self.target_file);
        let temp_output = self.temp_output.take().unwrap();
        let temp_path = temp_output.as_os_str().to_os_string();
        temp_output.persist(&self.target_file).with_context(|| {
            format!(
                "failed to rename temp file {:?} to output file {:?}",
                temp_path, self.target_file
            )
        })?;
        set_umasked_mode(&self.target_file, 0o666)
    }
}

impl Drop for Volume {
    fn drop(&mut self) {
        // Close the builder file first, if any
        self.builder.take();

        // TODO It would be nice to have some kind of wrapper with .wait and Drop::drop.
        if let Some(mut subprocess) = self.subprocess.take() {
            log::warn!("Shouldn't happen: killing subprocess {}", subprocess.id());
            let _ = subprocess.kill();
        }
    }
}

fn print_header(volume_name: &str, header: &tar::Header) -> io::Result<()> {
    let stderr = io::stderr();
    let mut stderr = stderr.lock();
    let local_datetime = chrono::Local {}.timestamp_millis((1000 * header.mtime().unwrap()) as _);
    let size_str = match header.entry_type() {
        tar::EntryType::Block | tar::EntryType::Char => format!(
            "{}:{}",
            header.device_major().unwrap().unwrap_or(0),
            header.device_minor().unwrap().unwrap_or(0),
        ),
        _ => format!("{}", header.size().unwrap()),
    };
    write!(
        stderr,
        "{marker} {type}{mod} {user} {group} {size:>12} {timestamp} {path}",
        marker = volume_name,
        type = entry_type_char(header),
        mod = decode_mod(header.mode().unwrap()),
        user = String::from_utf8_lossy(header.username_bytes().unwrap()),
        group = String::from_utf8_lossy(header.groupname_bytes().unwrap()),
        size = size_str,
        timestamp = local_datetime.format("%Y-%m-%d %H:%M:%S"),
        path = String::from_utf8_lossy(&header.path_bytes()),
    )?;

    let link = header.link_name_bytes();
    // We presume that link exist only for reason.
    let link = link.as_ref().map(|c| String::from_utf8_lossy(c));
    match header.entry_type() {
        tar::EntryType::Link => {
            write!(
                stderr,
                " link to {}",
                // We might just call unwrap and fail.
                link.unwrap_or_default(),
            )?;
        }
        tar::EntryType::Symlink => {
            write!(
                stderr,
                " -> {}",
                // We might just call unwrap and fail.
                link.unwrap_or_default(),
            )?;
        }
        _ => {}
    }
    writeln!(stderr)?;
    Ok(())
}

fn format_flag_group(group: u32) -> &'static str {
    match group {
        0 => "---",
        1 => "--x",
        2 => "-w-",
        3 => "-wx",
        4 => "r--",
        5 => "r-x",
        6 => "rw-",
        7 => "rwx",
        _ => unreachable!(),
    }
}

fn decode_mod(mode: u32) -> String {
    let mut res = String::with_capacity(9);
    // TODO sticky bits...
    for offset in [6u32, 3, 0].iter() {
        res.push_str(format_flag_group((mode >> offset) & 0x7));
    }
    res
}

fn entry_type_char(header: &tar::Header) -> char {
    match header.entry_type() {
        tar::EntryType::Regular | tar::EntryType::Continuous | tar::EntryType::GNUSparse => {
            if header.path_bytes().ends_with(&[b'/']) {
                'd'
            } else {
                '-'
            }
        }
        tar::EntryType::Link => 'h',
        tar::EntryType::Symlink => 'l',
        tar::EntryType::Char => 'c',
        tar::EntryType::Block => 'b',
        tar::EntryType::Directory => 'd',
        tar::EntryType::Fifo => 'p',
        tar::EntryType::GNULongName | tar::EntryType::GNULongLink => 'L',
        _ => '?',
    }
}

struct SplitState {
    vol_idx: usize,
    args: Args,
    dirs: patricia_tree::PatriciaMap<Box<tar::Header>>,
    // We keep it optional, as we take and set back.
    // I.e. it is optional only *within* certain functions.
    volume: Option<Volume>,
    interrupt_flag: Arc<AtomicBool>,
}

impl SplitState {
    fn new(args: Args, interrupt_flag: Arc<AtomicBool>) -> ah::Result<Self> {
        let vol_idx = 0;
        let volume = Volume::new(vol_idx, &args, interrupt_flag.clone())?;

        Ok(Self {
            vol_idx,
            args,
            dirs: Default::default(),
            volume: Some(volume),
            interrupt_flag,
        })
    }

    fn next_file<R: io::Read>(&mut self, mut entry: tar::Entry<R>) -> Result<()> {
        let volume = self.volume.as_mut().unwrap();
        let acc_size = volume.acc_size;
        let max_size = self.args.max_size;
        let entry_size = TAR_HEADER_SIZE + entry.header().entry_size().unwrap();

        if self.args.fail_on_large_file && entry_size > max_size {
            return Err(Error::FileTooLarge(
                String::from_utf8_lossy(&entry.path_bytes()).to_string(),
            ));
        }

        if acc_size > 0 && acc_size + entry_size > max_size {
            self.start_new_volume()?;
        }

        let volume = self.volume.as_mut().unwrap();
        let header = entry.header().clone();

        if self.args.recreate_dirs {
            let path_bytes = header.path_bytes();
            let mut path = path_bytes.deref();

            log::debug!("Checking path {:?}", String::from_utf8_lossy(path));
            let same_dir = path
                .strip_prefix(volume.prev_dir.as_slice())
                .map(|p| !p.is_empty() && !p.contains(&b'/'))
                .unwrap_or(false);
            if !same_dir {
                if let Some(p) = path.strip_suffix(&[b'/']) {
                    path = p;
                }

                let slash_pos = path.iter().enumerate().rev().find(|(_, &c)| c == b'/');
                if let Some((pos, _)) = slash_pos {
                    // std::path::Path is OS-dependent and cannot be used.  It would be
                    // nice to have something like Python's posixpath.
                    let dirname = &path[..=pos];

                    volume.inject_dirs_for_path(dirname, &self.dirs, self.args.verbose)?;
                    volume.prev_dir = dirname.to_vec();
                }
            } else {
                log::debug!("Dirname is same, skip it.")
            }
        }

        volume.write_data(&header, &mut entry, self.args.verbose)?;

        if self.args.recreate_dirs && header.entry_type().is_dir() {
            self.dirs
                .insert(header.path_bytes(), Box::new(entry.header().clone()));
            volume.stored_dirs.insert(header.path_bytes());
        }

        Ok(())
    }

    fn start_new_volume(&mut self) -> ah::Result<()> {
        self.volume.take().unwrap().finish()?;
        self.vol_idx += 1;
        self.volume = Some(Volume::new(
            self.vol_idx,
            &self.args,
            self.interrupt_flag.clone(),
        )?);

        Ok(())
    }

    fn finish(mut self) -> ah::Result<()> {
        self.volume.take().unwrap().finish()
    }
}

fn run(args: Args, interrupt_flag: Arc<AtomicBool>) -> Result<()> {
    let stdin = io::stdin();
    let stdin = stdin.lock();

    let file: Box<dyn io::Read> = if args.input_file == Path::new("-") {
        Box::new(stdin)
    } else {
        std::mem::drop(stdin);
        Box::new(io::BufReader::new(std::fs::File::open(&args.input_file)?))
    };
    let mut archive = tar::Archive::new(Interruptable::new(file, interrupt_flag.clone()));

    let mut state = SplitState::new(args, interrupt_flag)?;
    for ent in archive.entries()?.raw(false) {
        let ent = ent?;
        log::debug!("entry: {:?}@{}", ent.path()?, ent.size());
        state.next_file(ent)?;
    }
    state.finish()?;

    Ok(())
}

/// tempfile crate creates files that only owner can read; we reset
/// the file permissions to a default mode.
#[cfg(unix)]
fn set_umasked_mode(file: &Path, mode: u32) -> ah::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    // Is safe as we just set and reset umask.
    // It can lead to race condition in the multithreading
    // context, however.  Technically, this function should be
    // declared unsafe too, but it is not a library code.
    //
    // N.B. On Linux, one can get own umask by reading the `/proc/self/status`
    // file.
    let umask = unsafe {
        let umask = libc::umask(0);
        libc::umask(umask);
        umask
    };
    let result_mode = mode & (!umask as u32);
    std::fs::set_permissions(file, std::fs::Permissions::from_mode(result_mode)).with_context(
        || {
            format!(
                "failed to set permission {} to the output file {:?}",
                result_mode, file
            )
        },
    )?;
    Ok(())
}

#[cfg(not(unix))]
fn set_umasked_mode(file: &Path, _mode: u32) -> ah::Result<()> {
    // I have no better idea.
    log::warn!(
        "tempfile permissions on the output path {:?} haven't been changed on this OS",
        file
    );
    Ok(())
}

fn eprintln_error<E: std::fmt::Debug>(e: E) {
    use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor as _};

    let choice = if atty::is(atty::Stream::Stdout) {
        ColorChoice::Auto
    } else {
        ColorChoice::Never
    };
    let mut stderr = StandardStream::stderr(choice);

    let mut bold_red = ColorSpec::new();
    bold_red.set_fg(Some(Color::Red)).set_bold(true);

    stderr.set_color(&bold_red).unwrap();
    eprint!("error:");
    stderr.set_color(&ColorSpec::new()).unwrap();
    eprintln!(" {:?}", e);
}

fn main() {
    let interrupt_flag = Arc::new(AtomicBool::new(false));

    env_logger::init();
    let args = Args::parse();

    log::debug!("Args: {:?}", args);

    #[cfg(not(target_arch = "wasm32"))]
    {
        let interrput_flag2 = interrupt_flag.clone();
        let res = ctrlc::set_handler(move || {
            interrput_flag2.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        if let Err(e) = res {
            log::error!("failed to set SIGINT handler: {}. Ignoring...", e);
        }
    }

    if let Err(e) = run(args, interrupt_flag) {
        let retcode = match &e {
            Error::FileTooLarge(_) => 3,
            _ => 1,
        };
        // Convert to ah::Erorr for pretty output.
        eprintln_error(Into::<ah::Error>::into(e));
        exit(retcode);
    }
}
