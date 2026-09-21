//! Single-thread Type 2 benchmark; parsing and serialization are outside timing.
use gdelt_type1::{Fragment, Type2Options, reconstruct_type2};
use sha2::{Digest, Sha256};
use std::{fs::File, io::BufReader, time::Instant};

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let groups: Vec<Vec<Fragment>> =
        serde_json::from_reader(BufReader::new(File::open(&args[1]).unwrap())).unwrap();
    let runs: usize = args.get(2).map_or(5, |s| s.parse().unwrap());
    let mut seconds = vec![];
    let mut checksum = String::new();
    let mut slowest = vec![];
    for _ in 0..runs {
        let start = Instant::now();
        let mut timings = vec![];
        let results: Vec<_> = groups
            .iter()
            .enumerate()
            .map(|(i, g)| {
                let t = Instant::now();
                let r = reconstruct_type2(g.clone(), Type2Options::default());
                timings.push((i, t.elapsed().as_secs_f64()));
                r
            })
            .collect();
        seconds.push(start.elapsed().as_secs_f64());
        let mut hash = Sha256::new();
        for r in &results {
            hash.update(serde_json::to_vec(r).unwrap());
            hash.update([0]);
        }
        let current = format!("{:x}", hash.finalize());
        assert!(
            checksum.is_empty() || checksum == current,
            "nondeterministic output"
        );
        checksum = current;
        timings.sort_by(|a, b| b.1.total_cmp(&a.1));
        slowest = timings.into_iter().take(10).collect::<Vec<_>>();
        if let Some(path) = args.get(3) {
            serde_json::to_writer(File::create(path).unwrap(), &results).unwrap();
        }
    }
    println!(
        "{}",
        serde_json::json!({"version":env!("CARGO_PKG_VERSION"),"seconds":seconds,"recovery_sha256":checksum,"articles":groups.len(),"fragments":groups.iter().map(Vec::len).sum::<usize>(),"slowest_last_run":slowest})
    );
}
