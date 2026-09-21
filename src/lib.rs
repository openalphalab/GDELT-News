//! Type 1 maximum-overlap recovery, following Fronzetti Colladon & Vestrelli
//! (2026), https://doi.org/10.3390/bdcc10020045 and gdeltnews (GPL-3.0).
//! Prefix/suffix tries replace repeated scans of all remaining fragments.
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use unicode_normalization::{IsNormalized, UnicodeNormalization, is_nfc_quick};

pub mod type2;
pub mod type2_best_effort;
mod type2_index;
pub use type2::{Type2Diagnostics, Type2Options, reconstruct_type2};

pub const ALGORITHM: &str = "type1-trie-v1";
pub const TYPE2_ALGORITHM: &str = "type2-adaptive-overlap-v3";
pub const PIPELINE_VERSION: &str = "webngrams-v4";

pub(crate) fn normalize_nfc(text: &mut String) {
    if is_nfc_quick(text.chars()) != IsNormalized::Yes {
        *text = text.nfc().collect();
    }
}

#[derive(Debug, Deserialize)]
pub struct Record {
    pub date: String,
    pub url: String,
    pub lang: String,
    #[serde(rename = "type")]
    pub kind: u8,
    pub pos: u8,
    pub pre: String,
    pub ngram: String,
    pub post: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Fragment {
    pub pos: u8,
    pub text: String,
}

impl Record {
    pub fn fragment(&self, strip_artifacts: bool) -> Fragment {
        Fragment::from_context(
            self.kind,
            self.pos,
            &self.pre,
            &self.ngram,
            &self.post,
            strip_artifacts,
        )
    }
}

impl Fragment {
    pub fn from_context(
        kind: u8,
        pos: u8,
        pre: &str,
        ngram: &str,
        post: &str,
        strip_artifacts: bool,
    ) -> Self {
        if kind == 2 {
            // Join before normalization/segmentation: a combining sequence may
            // straddle a KWIC field boundary. Never trim or insert whitespace.
            let mut text = format!("{pre}{ngram}{post}");
            normalize_nfc(&mut text);
            return Fragment { pos, text };
        }
        let text = [pre, ngram, post]
            .into_iter()
            .flat_map(str::split_whitespace)
            .collect::<Vec<_>>()
            .join(" ");
        let text = if strip_artifacts && pos < 20 {
            text.split_once(" / ")
                .map_or(text.clone(), |(_, tail)| tail.to_owned())
        } else {
            text
        };
        Fragment { pos, text }
    }
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Unmerged {
    pub pos: u8,
    pub text: String,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Recovery {
    pub text: String,
    pub input_fragments: usize,
    pub merged_fragments: usize,
    pub unmerged: Vec<Unmerged>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type2_diagnostics: Option<Type2Diagnostics>,
}

#[derive(Default)]
struct Node {
    candidates: Vec<usize>,
    cursor: usize,
}

struct Index {
    edges: FxHashMap<(usize, u32), usize>,
    nodes: Vec<Node>,
}

impl Index {
    fn new(words: &[Vec<u32>], reverse: bool) -> Self {
        let mut out = Self {
            edges: FxHashMap::default(),
            nodes: vec![Node::default()],
        };
        for (i, word) in words.iter().enumerate().skip(1) {
            let mut node = 0;
            for step in 0..word.len() {
                let token = if reverse {
                    word[word.len() - 1 - step]
                } else {
                    word[step]
                };
                node = *out.edges.entry((node, token)).or_insert_with(|| {
                    out.nodes.push(Node::default());
                    out.nodes.len() - 1
                });
                out.nodes[node].candidates.push(i);
            }
        }
        out
    }

    fn candidate(
        &mut self,
        tokens: impl Iterator<Item = u32>,
        used: &[bool],
        fragments: &[Fragment],
        boundary: u8,
        prepend: bool,
    ) -> Option<usize> {
        let mut node = 0;
        for token in tokens {
            node = *self.edges.get(&(node, token))?;
        }
        let node = &mut self.nodes[node];
        while let Some(&i) = node.candidates.get(node.cursor) {
            // Bounds only move outward, so an ineligible entry stays ineligible.
            let eligible = if prepend {
                fragments[i].pos <= boundary
            } else {
                fragments[i].pos >= boundary
            };
            if !used[i] && eligible {
                return Some(i);
            }
            node.cursor += 1;
        }
        None
    }
}

/// Stable positional ordering, maximum word overlap, lowest input index on
/// ties, and append before prepend. No quotation marks or repeated phrases
/// are removed. Every unused fragment is reported separately.
pub fn reconstruct(mut fragments: Vec<Fragment>) -> Recovery {
    fragments.sort_by_key(|f| f.pos);
    if fragments.is_empty() {
        return Recovery {
            text: String::new(),
            input_fragments: 0,
            merged_fragments: 0,
            unmerged: vec![],
            type2_diagnostics: None,
        };
    }
    let mut vocabulary = FxHashMap::default();
    let mut tokens = Vec::new();
    let words: Vec<Vec<u32>> = fragments
        .iter()
        .map(|f| {
            f.text
                .split_whitespace()
                .map(|word| {
                    *vocabulary.entry(word).or_insert_with(|| {
                        tokens.push(word);
                        (tokens.len() - 1) as u32
                    })
                })
                .collect()
        })
        .collect();
    let cap = words.iter().map(Vec::len).max().unwrap_or(0);
    let mut prefixes = Index::new(&words, false);
    let mut suffixes = Index::new(&words, true);
    let mut used = vec![false; words.len()];
    used[0] = true;
    let mut result = VecDeque::from(words[0].clone());
    let mut min_pos = fragments[0].pos;
    let mut max_pos = min_pos;
    let mut merged = 1;
    loop {
        let limit = cap.min(result.len());
        let mut best = None;
        // Descending overlap lengths guarantee the maximum. Both directions
        // are checked before resolving the original input-order tie.
        for k in (1..=limit).rev() {
            let append = prefixes.candidate(
                (result.len() - k..result.len()).map(|j| result[j]),
                &used,
                &fragments,
                max_pos,
                false,
            );
            let prepend = suffixes.candidate(
                (0..k).rev().map(|j| result[j]),
                &used,
                &fragments,
                min_pos,
                true,
            );
            best = match (append, prepend) {
                (Some(a), Some(p)) if p < a => Some((p, k, true)),
                (Some(a), _) => Some((a, k, false)),
                (_, Some(p)) => Some((p, k, true)),
                _ => None,
            };
            if best.is_some() {
                break;
            }
        }
        let Some((i, overlap, prepend)) = best else {
            break;
        };
        if prepend {
            for &word in words[i][..words[i].len() - overlap].iter().rev() {
                result.push_front(word);
            }
        } else {
            result.extend(&words[i][overlap..]);
        }
        used[i] = true;
        merged += 1;
        min_pos = min_pos.min(fragments[i].pos);
        max_pos = max_pos.max(fragments[i].pos);
    }
    Recovery {
        text: result
            .iter()
            .map(|&i| tokens[i as usize])
            .collect::<Vec<_>>()
            .join(" "),
        input_fragments: fragments.len(),
        merged_fragments: merged,
        unmerged: fragments
            .iter()
            .zip(used)
            .filter(|(_, used)| !used)
            .map(|(f, _)| Unmerged {
                pos: f.pos,
                text: f.text.clone(),
            })
            .collect(),
        type2_diagnostics: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Independent straightforward reference for positional greedy semantics.
    fn reference(mut fragments: Vec<Fragment>) -> String {
        fragments.sort_by_key(|f| f.pos);
        let words: Vec<Vec<&str>> = fragments
            .iter()
            .map(|f| f.text.split_whitespace().collect())
            .collect();
        if words.is_empty() {
            return String::new();
        }
        let mut result = words[0].clone();
        let mut used = vec![false; words.len()];
        used[0] = true;
        let mut lo = fragments[0].pos;
        let mut hi = lo;
        loop {
            let mut best = (0, 0, false);
            for i in 1..words.len() {
                if used[i] {
                    continue;
                }
                let w = &words[i];
                for k in (1..=w.len().min(result.len())).rev() {
                    if k <= best.1 {
                        break;
                    }
                    if fragments[i].pos >= hi && result[result.len() - k..] == w[..k] {
                        best = (i, k, false);
                        break;
                    }
                    if fragments[i].pos <= lo && result[..k] == w[w.len() - k..] {
                        best = (i, k, true);
                        break;
                    }
                }
            }
            let (i, k, prepend) = best;
            if k == 0 {
                break;
            }
            if prepend {
                result.splice(..0, words[i][..words[i].len() - k].iter().copied());
            } else {
                result.extend_from_slice(&words[i][k..]);
            }
            used[i] = true;
            lo = lo.min(fragments[i].pos);
            hi = hi.max(fragments[i].pos);
        }
        result.join(" ")
    }

    #[test]
    fn matches_reference_on_repetitive_random_fragments() {
        let mut state = 71u64;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            state >> 32
        };
        for _ in 0..2500 {
            let mut fragments = vec![];
            for _ in 0..next() % 50 {
                let pos = (next() % 10 * 10) as u8;
                let text = (0..next() % 18)
                    .map(|_| format!("w{}", next() % 8))
                    .collect::<Vec<_>>()
                    .join(" ");
                fragments.push(Fragment { pos, text });
            }
            assert_eq!(reconstruct(fragments.clone()).text, reference(fragments));
        }
    }

    #[test]
    fn keeps_quotes_unicode_and_disconnected_fragments() {
        let r = reconstruct(vec![
            Fragment {
                pos: 0,
                text: "Café \"hello\" | world".into(),
            },
            Fragment {
                pos: 10,
                text: "| world is here".into(),
            },
            Fragment {
                pos: 90,
                text: "unconnected ending".into(),
            },
        ]);
        assert_eq!(r.text, "Café \"hello\" | world is here");
        assert_eq!(r.merged_fragments, 2);
        assert_eq!(r.unmerged[0].text, "unconnected ending");
    }
}
