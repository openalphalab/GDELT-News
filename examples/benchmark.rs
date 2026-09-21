use gdelt_type1::{Fragment, reconstruct};
use sha2::{Digest, Sha256};
use std::{fs::File, time::Instant};

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("prepared fragment JSON path");
    let groups: Vec<Vec<Fragment>> = serde_json::from_reader(File::open(path).unwrap()).unwrap();
    let mut seconds = vec![];
    let mut checksum = String::new();
    for _ in 0..3 {
        let start = Instant::now();
        let results: Vec<_> = groups.iter().map(|g| reconstruct(g.clone())).collect();
        seconds.push(start.elapsed().as_secs_f64());
        let mut hash = Sha256::new();
        for r in results {
            hash.update(r.text.as_bytes());
            hash.update([0]);
        }
        checksum = format!("{:x}", hash.finalize());
    }
    println!(
        "{}",
        serde_json::json!({"seconds":seconds,"text_sha256":checksum,"articles":groups.len()})
    );
}
