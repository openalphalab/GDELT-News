//! Exact suffix/prefix matching with failure links and compact candidate ranges.
use rustc_hash::FxHashMap;

pub(super) enum Lookup {
    Anchors(AnchorIndex),
    Dense(OverlapIndex),
}

pub(super) struct AnchorIndex {
    ranges: FxHashMap<[u32; 4], (usize, usize)>,
    candidates: Vec<u32>,
    pub(super) width: usize,
}

impl Lookup {
    pub(super) fn new(words: &[Vec<u32>], reverse: bool, minimum: usize) -> Self {
        let width = minimum.min(4);
        let signature = |w: &[u32]| {
            let mut key = [u32::MAX; 4];
            for j in 0..width {
                key[j] = w[if reverse { w.len() - 1 - j } else { j }];
            }
            key
        };
        let mut ranges = FxHashMap::<_, (usize, usize)>::default();
        for w in words.iter().skip(1).filter(|w| w.len() >= minimum) {
            let entry = ranges.entry(signature(w)).or_default();
            entry.1 += 1;
            // Bound candidate comparisons on low-entropy/repetitive input.
            if entry.1 > 256 {
                return Self::Dense(OverlapIndex::new(words, reverse));
            }
        }
        let mut total = 0;
        for (start, count) in ranges.values_mut() {
            *start = total;
            total += *count;
            *count = 0;
        }
        let mut candidates = vec![0; total];
        for (i, w) in words
            .iter()
            .enumerate()
            .skip(1)
            .filter(|(_, w)| w.len() >= minimum)
        {
            let (start, count) = ranges.get_mut(&signature(w)).unwrap();
            candidates[*start + *count] = u32::try_from(i).expect("too many Type 2 fragments");
            *count += 1;
        }
        Self::Anchors(AnchorIndex {
            ranges,
            candidates,
            width,
        })
    }
}

impl AnchorIndex {
    pub(super) fn candidates(&self, key: &[u32; 4]) -> &[u32] {
        self.ranges.get(key).map_or(&[], |&(start, count)| {
            &self.candidates[start..start + count]
        })
    }
}

#[derive(Default)]
struct Node {
    depth: u32,
    failure: u32,
    start: u32,
    count: u32,
}

pub(super) struct OverlapIndex {
    edges: FxHashMap<u64, u32>,
    nodes: Vec<Node>,
    candidates: Vec<u32>,
}

fn key(node: u32, token: u32) -> u64 {
    (u64::from(node) << 32) | u64::from(token)
}

impl OverlapIndex {
    pub(super) fn new(words: &[Vec<u32>], reverse: bool) -> Self {
        let mut edges = FxHashMap::default();
        let mut nodes = vec![Node::default()];
        for word in words.iter().skip(1) {
            let mut node = 0;
            for j in 0..word.len() {
                let token = word[if reverse { word.len() - 1 - j } else { j }];
                let depth = nodes[node as usize].depth + 1;
                node = *edges.entry(key(node, token)).or_insert_with(|| {
                    let child =
                        u32::try_from(nodes.len()).expect("Type 2 index exceeds u32 capacity");
                    nodes.push(Node {
                        depth,
                        ..Node::default()
                    });
                    child
                });
                nodes[node as usize].count += 1;
            }
        }
        let mut total = 0u32;
        for node in &mut nodes {
            node.start = total;
            total = total
                .checked_add(node.count)
                .expect("Type 2 candidate index exceeds u32 capacity");
            node.count = 0;
        }
        let mut candidates = vec![0; total as usize];
        for (i, word) in words.iter().enumerate().skip(1) {
            let mut node = 0;
            for j in 0..word.len() {
                let token = word[if reverse { word.len() - 1 - j } else { j }];
                node = edges[&key(node, token)];
                let n = &mut nodes[node as usize];
                candidates[(n.start + n.count) as usize] =
                    u32::try_from(i).expect("too many Type 2 fragments");
                n.count += 1;
            }
        }
        // Temporary adjacency arrays avoid one allocation per node.
        let mut heads = vec![u32::MAX; nodes.len()];
        let mut next = vec![u32::MAX; nodes.len()];
        let mut labels = vec![0; nodes.len()];
        for (&edge, &child) in &edges {
            let parent = (edge >> 32) as usize;
            next[child as usize] = heads[parent];
            heads[parent] = child;
            labels[child as usize] = edge as u32;
        }
        let mut queue = vec![0];
        let mut cursor = 0;
        while cursor < queue.len() {
            let parent = queue[cursor];
            cursor += 1;
            let mut child = heads[parent as usize];
            while child != u32::MAX {
                if parent != 0 {
                    let token = labels[child as usize];
                    let mut failure = nodes[parent as usize].failure;
                    loop {
                        if let Some(&destination) = edges.get(&key(failure, token)) {
                            nodes[child as usize].failure = destination;
                            break;
                        }
                        if failure == 0 {
                            break;
                        }
                        failure = nodes[failure as usize].failure;
                    }
                }
                queue.push(child);
                child = next[child as usize];
            }
        }
        Self {
            edges,
            nodes,
            candidates,
        }
    }

    /// Longest suffix of `text` that is an indexed fragment prefix.
    pub(super) fn suffix(&self, text: impl Iterator<Item = u32>) -> u32 {
        let mut node = 0;
        for token in text {
            loop {
                if let Some(&child) = self.edges.get(&key(node, token)) {
                    node = child;
                    break;
                }
                if node == 0 {
                    break;
                }
                node = self.nodes[node as usize].failure;
            }
        }
        node
    }

    pub(super) fn depth(&self, node: u32) -> usize {
        self.nodes[node as usize].depth as usize
    }

    pub(super) fn shorter(&self, node: u32) -> u32 {
        self.nodes[node as usize].failure
    }

    pub(super) fn candidates(&self, node: u32) -> &[u32] {
        let n = &self.nodes[node as usize];
        &self.candidates[n.start as usize..(n.start + n.count) as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_chain_equals_exhaustive_suffix_prefix_matching() {
        let mut seed = 931u64;
        let mut next = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            (seed >> 32) as u32
        };
        for _ in 0..400 {
            let words: Vec<Vec<u32>> = (0..25)
                .map(|_| (0..next() % 25).map(|_| next() % 6).collect())
                .collect();
            let text: Vec<u32> = (0..next() % 40).map(|_| next() % 6).collect();
            for reverse in [false, true] {
                let index = OverlapIndex::new(&words, reverse);
                let mut input = text.clone();
                if reverse {
                    input.reverse();
                }
                let mut actual = vec![];
                let mut node = index.suffix(input.iter().copied());
                while node != 0 {
                    actual.push((index.depth(node), index.candidates(node).to_vec()));
                    node = index.shorter(node);
                }
                let mut expected = vec![];
                for k in (1..=input.len()).rev() {
                    let ids: Vec<_> = words
                        .iter()
                        .enumerate()
                        .skip(1)
                        .filter_map(|(i, w)| {
                            let mut word = w.clone();
                            if reverse {
                                word.reverse();
                            }
                            (word.len() >= k && word[..k] == input[input.len() - k..])
                                .then_some(i as u32)
                        })
                        .collect();
                    if !ids.is_empty() {
                        expected.push((k, ids));
                    }
                }
                assert_eq!(actual, expected);
            }
        }
    }
}
