//! Optional readable assembly of all recovered sections. Ordering is estimated.
use crate::type2::RecoveredSegment;
use rustc_hash::FxHashMap;
use serde::Serialize;
use std::collections::VecDeque;
use unicode_segmentation::UnicodeSegmentation;

#[derive(Debug, Serialize, PartialEq)]
pub struct EstimatedJoin {
    /// 0 is the conservative main text; 1.. refers to residual sections.
    pub section: usize,
    pub method: &'static str,
    pub overlap_graphemes: usize,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct BestEffortAssembly {
    pub text: String,
    pub ordering: &'static str,
    pub minimum_overlap: usize,
    pub joins: Vec<EstimatedJoin>,
    pub position_only_joins: usize,
    pub comparison_budget_exhausted: bool,
}

fn prefix(pattern: &[u32]) -> Vec<usize> {
    let mut pi = vec![0; pattern.len()];
    for i in 1..pattern.len() {
        let mut j = pi[i - 1];
        while j > 0 && pattern[i] != pattern[j] {
            j = pi[j - 1];
        }
        if pattern[i] == pattern[j] {
            j += 1;
        }
        pi[i] = j;
    }
    pi
}

// Exact KMP: returns containment or the maximal suffix/prefix overlap.
fn match_text(text: &[u32], pattern: &[u32], pi: &[usize]) -> (bool, usize) {
    if pattern.is_empty() {
        return (true, 0);
    }
    let mut j = 0;
    for &token in text {
        while j > 0 && token != pattern[j] {
            j = pi[j - 1];
        }
        if token == pattern[j] {
            j += 1;
        }
        if j == pattern.len() {
            return (true, j);
        }
    }
    (false, j)
}

/// Preserve every section as a substring. Unsupported connections are separated
/// by paragraph breaks and explicitly reported as positional guesses.
pub fn assemble_best_effort(
    primary: &str,
    primary_pos: u8,
    segments: &[RecoveredSegment],
    minimum: usize,
) -> BestEffortAssembly {
    let mut vocabulary = FxHashMap::default();
    let mut tokens = vec![];
    let mut parts = vec![(primary_pos, primary)];
    parts.extend(segments.iter().map(|s| (s.min_pos, s.text.as_str())));
    let words: Vec<Vec<u32>> = parts
        .iter()
        .map(|(_, text)| {
            text.graphemes(true)
                .map(|g| {
                    *vocabulary.entry(g).or_insert_with(|| {
                        tokens.push(g);
                        (tokens.len() - 1) as u32
                    })
                })
                .collect()
        })
        .collect();
    let failure: Vec<_> = words.iter().map(|w| prefix(w)).collect();
    let mut pending: Vec<_> = (0..parts.len()).collect();
    pending.sort_by_key(|&i| (parts[i].0, i));
    let mut pending: VecDeque<_> = pending.into();
    let first = pending.pop_front().unwrap();
    let mut output = words[first].clone();
    let mut text = parts[first].1.to_owned();
    let mut joins = vec![EstimatedJoin {
        section: first,
        method: "seed",
        overlap_graphemes: 0,
    }];
    let mut position_only_joins = 0;
    let mut budget = 20_000_000usize;
    let mut exhausted = false;
    while !pending.is_empty() {
        let mut best = None;
        for (slot, &i) in pending.iter().take(64).enumerate() {
            let cost = output.len() + words[i].len();
            if cost > budget {
                exhausted = true;
                break;
            }
            budget -= cost;
            let (contained, overlap) = match_text(&output, &words[i], &failure[i]);
            let meaningful = words[i][..overlap]
                .iter()
                .any(|&id| tokens[id as usize].chars().any(char::is_alphanumeric));
            if contained || (overlap >= minimum && meaningful) {
                let score = (contained, overlap, usize::MAX - slot);
                if best
                    .as_ref()
                    .is_none_or(|&(_, _, _, previous)| score > previous)
                {
                    best = Some((slot, contained, overlap, score));
                }
            }
        }
        let (slot, contained, overlap) = best.map_or((0, false, 0), |(s, c, k, _)| (s, c, k));
        let i = pending.remove(slot).unwrap();
        let method = if contained {
            "contained"
        } else if overlap > 0 {
            "overlap_estimate"
        } else {
            "position_only"
        };
        if !contained {
            if overlap == 0 {
                // A sentinel forbids future matches across an unsupported join.
                output.push(u32::MAX);
                text.push_str("\n\n");
                position_only_joins += 1;
            }
            output.extend_from_slice(&words[i][overlap..]);
            for &id in &words[i][overlap..] {
                text.push_str(tokens[id as usize]);
            }
        }
        joins.push(EstimatedJoin {
            section: i,
            method,
            overlap_graphemes: overlap,
        });
    }
    BestEffortAssembly {
        text,
        minimum_overlap: minimum,
        ordering: if segments.is_empty() {
            "same_as_conservative"
        } else {
            "estimated"
        },
        joins,
        position_only_joins,
        comparison_budget_exhausted: exhausted,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn section(pos: u8, text: &str) -> RecoveredSegment {
        RecoveredSegment {
            text: text.into(),
            min_pos: pos,
            max_pos: pos,
            fragment_indices: vec![],
            stop_reason: "no_confident_overlap",
        }
    }
    #[test]
    fn combines_overlaps_containment_and_gaps_without_losing_sections() {
        let sections = vec![
            section(0, "甲乙丙丁戊己"),
            section(0, "戊己庚辛"),
            section(20, "另一条独立材料"),
        ];
        let r = assemble_best_effort("序言甲乙丙丁戊己", 0, &sections, 2);
        assert_eq!(r.text, "序言甲乙丙丁戊己庚辛\n\n另一条独立材料");
        assert_eq!(r.position_only_joins, 1);
        assert_eq!(r.ordering, "estimated");
        assert!(sections.iter().all(|s| r.text.contains(&s.text)));
    }
    #[test]
    fn overlap_requires_whole_graphemes_and_substantive_content() {
        let sections = vec![section(0, "้ต่อไป"), section(20, "....next")];
        let r = assemble_best_effort("ข่าวก้", 0, &sections, 1);
        assert_eq!(r.position_only_joins, 2);
        let r = assemble_best_effort("first....", 0, &[section(0, "....next")], 1);
        assert_eq!(r.position_only_joins, 1);
    }
}
