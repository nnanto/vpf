use std::path::PathBuf;

use clap::Parser;
use vpf_fuse::VpfFs;

#[derive(Debug, Parser)]
#[command(
    name = "vpf-fuse",
    about = "Mount a VPF store as a read-only filesystem"
)]
struct Args {
    /// Path to the VPF store directory.
    store_dir: PathBuf,
    /// Mount point (must already exist).
    mountpoint: PathBuf,
}

fn main() {
    let args = Args::parse();
    let fs = match VpfFs::open(&args.store_dir) {
        Ok(fs) => fs,
        Err(error) => {
            eprintln!(
                "vpf-fuse: opening store {}: {error}",
                args.store_dir.display()
            );
            std::process::exit(1);
        }
    };

    let options = [
        fuser::MountOption::RO,
        fuser::MountOption::FSName("vpf".to_owned()),
        fuser::MountOption::DefaultPermissions,
    ];
    if let Err(error) = fuser::mount2(fs, &args.mountpoint, &options) {
        eprintln!("vpf-fuse: mounting {}: {error}", args.mountpoint.display());
        std::process::exit(1);
    }
}
