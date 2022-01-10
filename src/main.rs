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
use clap::Parser;
use std::{
    ffi::OsString,
    fs::File,
    io,
    path::{Path, PathBuf},
    process::{exit, Child, Command, Stdio},
    str::FromStr,
};
use tar::Archive;

// Simple wrapper for binary one-letter units (like 300G).
fn clap_parse_size(src: &str) -> Result<u64, parse_size::Error> {
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
    #[clap(long)]
    compress: Option<String>,
    #[clap(short = 'a', long, default_value = "5")]
    suffix_length: u8,
    #[clap(help = "input file path or `-` for stdin")]
    input_file: PathBuf,
    output_prefix: String,
}

struct SplitState {
    acc_size: u64,
    vol_idx: u32,
    args: Args,
    // TODO: tar output + compress
    builder: Option<tar::Builder<Box<dyn io::Write>>>,
    subprocess: Option<Child>,
}

impl SplitState {
    fn new(args: Args) -> Self {
        Self {
            acc_size: 0,
            vol_idx: 0,
            args,
            builder: None,
            subprocess: None,
        }
    }

    fn next_file<R: io::Read>(&mut self, mut entry: tar::Entry<R>) -> io::Result<()> {
        let acc_size = self.acc_size;
        let max_size = self.args.max_size;
        let mut builder = match &mut self.builder {
            Some(builder) => builder,
            None => self.start_new_volume()?,
        };
        if acc_size > 0 && acc_size + entry.size() > max_size {
            builder = self.start_new_volume()?;
        }

        let header = entry.header().clone();
        builder.append(&header, &mut entry)?;

        self.acc_size += entry.size();
        Ok(())
    }

    fn start_new_volume(&mut self) -> io::Result<&mut tar::Builder<Box<dyn io::Write>>> {
        self.finish()?;
        let out_path = format!(
            "{path}{index:0>width$}",
            path = self.args.output_prefix,
            width = self.args.suffix_length as _,
            index = self.vol_idx,
        );
        log::info!("Starting new volume: {}", out_path);
        // TODO one should write to the file a command output if available, and then
        // output tar to the command input.
        let out_file = File::create(out_path)?;
        let out_file = match &self.args.compress {
            Some(compress) => {
                let shell = std::env::var_os("SHELL").unwrap_or_else(|| {
                    OsString::from_str("/bin/bash").expect("internal: can't run on this os")
                });
                let subprocess = Command::new(shell)
                    .arg("-c")
                    .arg(compress)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::from(out_file))
                    .spawn()?;
                log::info!("Executing subprocess {}", subprocess.id());
                Box::new(io::BufWriter::with_capacity(
                    /* 16384 is default pipe buffer size for Linux;
                     on MacOS, it can grow on demand up to this value.
                     We are using half of this value.
                    */
                    1 << 13,
                    subprocess
                        .stdin
                        .expect("internal: expecting subprocess stdin"),
                )) as Box<dyn io::Write>
            }
            None => Box::new(io::BufWriter::new(out_file)),
        };
        let builder = tar::Builder::new(Box::new(out_file) as Box<dyn io::Write>);
        self.vol_idx += 1;
        self.acc_size = 0;
        Ok(self.builder.insert(builder))
    }

    fn finish(&mut self) -> io::Result<()> {
        if let Some(mut old_builder) = self.builder.take() {
            old_builder.finish()?;
            // It is important that we call the Builder::finish first
            if let Some(mut subprocess) = self.subprocess.take() {
                log::info!("Waiting subprocess {} to finish", subprocess.id());
                subprocess.wait()?;
            }
        }
        Ok(())
    }
}

impl Drop for SplitState {
    fn drop(&mut self) {
        if let Some(mut subprocess) = self.subprocess.take() {
            log::warn!("Shouldn't happen: killing subprocess {}", subprocess.id());
            subprocess
                .kill()
                .expect("failed to kill a child subprocess");
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();

    log::debug!("Args: {:?}", args);

    let stdin = io::stdin();
    let stdin = stdin.lock();

    let file: Box<dyn io::Read> = if args.input_file == Path::new("-") {
        Box::new(stdin)
    } else {
        std::mem::drop(stdin);
        Box::new(io::BufReader::new(std::fs::File::open(&args.input_file)?))
    };
    let mut archive = Archive::new(file);
    let mut state = SplitState::new(args);
    for ent in archive.entries()?.raw(true) {
        let ent = ent?;
        log::debug!("entry: {:?}@{}", ent.path()?, ent.size());
        state.next_file(ent)?;
    }
    state.finish()?;
    Ok(())
}

fn format_error<E: std::fmt::Display>(e: E) {
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
    eprintln!(" {}", e);
}

fn main() {
    if let Err(e) = run() {
        format_error(e);
        exit(1);
    }
}
