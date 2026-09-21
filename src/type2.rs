//! Evidence-preserving Type 2 assembly: independent boundaries, bounded
//! exhaustive branch search, and separately recovered residual segments.
use crate::{Fragment, Recovery, Unmerged, type2_index::Lookup};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;
use std::{cell::Cell, collections::VecDeque};
use unicode_segmentation::UnicodeSegmentation;

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Type2Options {
    pub min_overlap: usize,
    pub max_position_gap: u8,
    /// Total trial merges per article; zero disables branch search.
    pub search_budget: usize,
    pub best_effort: bool,
    pub best_effort_min_overlap: usize,
}

impl Default for Type2Options {
    fn default() -> Self {
        Self {
            min_overlap: 4,
            max_position_gap: 10,
            search_budget: 20_000,
            best_effort: false,
            best_effort_min_overlap: 2,
        }
    }
}

#[derive(Debug, Serialize, PartialEq)]
pub struct RecoveredSegment {
    pub text: String,
    pub min_pos: u8,
    pub max_pos: u8,
    /// Indices into all input fragments after stable positional sorting.
    pub fragment_indices: Vec<usize>,
    pub stop_reason: &'static str,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Type2Diagnostics {
    pub tokenization: &'static str,
    pub min_overlap: usize,
    pub max_position_gap: u8,
    pub minimum_overlap_used: Option<usize>,
    pub stop_reason: &'static str,
    pub conflicting_candidates: usize,
    pub search_budget: usize,
    pub search_steps: usize,
    pub search_status: &'static str,
    pub primary_fragment_indices: Vec<usize>,
    /// These sections have no asserted order relative to the primary text.
    /// They cover exactly the fragments also preserved in unmerged.
    pub segments: Vec<RecoveredSegment>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_effort: Option<crate::type2_best_effort::BestEffortAssembly>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Move {
    index: usize,
    overlap: usize,
    prepend: bool,
}

#[derive(Clone)]
struct State {
    text: VecDeque<u32>,
    used: Vec<bool>,
    indices: Vec<usize>,
    min_pos: u8,
    max_pos: u8,
    min_overlap: Option<usize>,
    used_count: usize,
    append_dead: Cell<bool>,
    prepend_dead: Cell<bool>,
}

impl State {
    fn seed(
        index: usize,
        mut used: Vec<bool>,
        previously_used: usize,
        words: &[Vec<u32>],
        fragments: &[Fragment],
    ) -> Self {
        used[index] = true;
        let used_count = previously_used + 1;
        Self {
            text: words[index].clone().into(),
            used,
            indices: vec![index],
            min_pos: fragments[index].pos,
            max_pos: fragments[index].pos,
            min_overlap: None,
            used_count,
            append_dead: Cell::new(false),
            prepend_dead: Cell::new(false),
        }
    }

    fn apply(&mut self, step: Move, words: &[Vec<u32>], fragments: &[Fragment], cap: usize) {
        let w = &words[step.index];
        if w.len() > step.overlap {
            if step.prepend || self.text.len() < cap {
                self.prepend_dead.set(false);
            }
            if !step.prepend || self.text.len() < cap {
                self.append_dead.set(false);
            }
        }
        if fragments[step.index].pos < self.min_pos {
            self.prepend_dead.set(false);
        }
        if fragments[step.index].pos > self.max_pos {
            self.append_dead.set(false);
        }
        if step.prepend {
            for &token in w[..w.len() - step.overlap].iter().rev() {
                self.text.push_front(token);
            }
        } else {
            self.text.extend(&w[step.overlap..]);
        }
        self.used[step.index] = true;
        self.used_count += 1;
        self.indices.push(step.index);
        self.min_pos = self.min_pos.min(fragments[step.index].pos);
        self.max_pos = self.max_pos.max(fragments[step.index].pos);
        self.min_overlap = Some(
            self.min_overlap
                .map_or(step.overlap, |k| k.min(step.overlap)),
        );
    }

    fn estimated_bytes(&self) -> usize {
        self.used.capacity()
            + self.text.capacity() * 4
            + self.indices.capacity() * size_of::<usize>()
            + 256
    }
}

// Exact equality, including the whole text, is required. Equal hashes alone
// never establish that two search states are interchangeable.
#[derive(Hash, PartialEq, Eq)]
struct SearchKey {
    text: VecDeque<u32>,
    used: Vec<bool>,
    min_pos: u8,
    max_pos: u8,
}

impl SearchKey {
    fn new(state: &State) -> Self {
        Self {
            text: state.text.clone(),
            used: state.used.clone(),
            min_pos: state.min_pos,
            max_pos: state.max_pos,
        }
    }
    fn estimated_bytes(&self) -> usize {
        self.text.capacity() * 4 + self.used.capacity() + 256
    }
}

enum Step {
    Safe(Move),
    Branch(Vec<Move>),
    End,
}
enum Search {
    Unique(State),
    Multiple,
    NoCompletion,
    Exhausted,
}

struct Engine<'a> {
    fragments: &'a [Fragment],
    words: Vec<Vec<u32>>,
    tokens: Vec<&'a str>,
    meaningful: Vec<bool>,
    prefixes: Lookup,
    suffixes: Lookup,
    cap: usize,
    options: Type2Options,
}

impl<'a> Engine<'a> {
    fn new(fragments: &'a [Fragment], options: Type2Options) -> Self {
        let mut vocabulary = FxHashMap::default();
        let mut tokens = Vec::new();
        let words: Vec<Vec<u32>> = fragments
            .iter()
            .map(|f| {
                f.text
                    .graphemes(true)
                    .map(|t| {
                        *vocabulary.entry(t).or_insert_with(|| {
                            tokens.push(t);
                            (tokens.len() - 1) as u32
                        })
                    })
                    .collect()
            })
            .collect();
        let meaningful = tokens
            .iter()
            .map(|g| g.chars().any(char::is_alphanumeric))
            .collect();
        let cap = words.iter().map(Vec::len).max().unwrap_or(0);
        let prefixes = Lookup::new(&words, false, options.min_overlap);
        let suffixes = Lookup::new(&words, true, options.min_overlap);
        Self {
            fragments,
            words,
            tokens,
            meaningful,
            prefixes,
            suffixes,
            cap,
            options,
        }
    }

    fn boundary(&self, state: &State, prepend: bool) -> Vec<Move> {
        let dead = if prepend {
            &state.prepend_dead
        } else {
            &state.append_dead
        };
        if dead.get() {
            return vec![];
        }
        let boundary = if prepend {
            state.min_pos
        } else {
            state.max_pos
        };
        let lo = if prepend {
            boundary.saturating_sub(self.options.max_position_gap)
        } else {
            boundary
        };
        let hi = if prepend {
            boundary
        } else {
            boundary.saturating_add(self.options.max_position_gap)
        };
        let limit = self.cap.min(state.text.len());
        let meaningful = (0..limit).find(|&j| {
            let p = if prepend { j } else { state.text.len() - 1 - j };
            self.meaningful[state.text[p] as usize]
        });
        let Some(meaningful) = meaningful else {
            dead.set(true);
            return vec![];
        };
        let minimum = self.options.min_overlap.max(meaningful + 1);
        let matching_moves = |indices: &[u32], k: usize, verify: bool| {
            let first = indices.partition_point(|&i| self.fragments[i as usize].pos < lo);
            let last = indices.partition_point(|&i| self.fragments[i as usize].pos <= hi);
            indices[first..last]
                .iter()
                .filter(|&&i| {
                    let i = i as usize;
                    if state.used[i] {
                        return false;
                    }
                    if !verify {
                        return true;
                    }
                    let w = &self.words[i];
                    w.len() >= k
                        && (0..k).all(|j| {
                            if prepend {
                                w[w.len() - k + j] == state.text[j]
                            } else {
                                w[j] == state.text[state.text.len() - k + j]
                            }
                        })
                })
                .map(|&i| Move {
                    index: i as usize,
                    overlap: k,
                    prepend,
                })
                .collect::<Vec<_>>()
        };
        let index = if prepend {
            &self.suffixes
        } else {
            &self.prefixes
        };
        match index {
            Lookup::Anchors(index) => {
                for k in (minimum..=limit).rev() {
                    let mut key = [u32::MAX; 4];
                    for (j, token) in key.iter_mut().enumerate().take(index.width) {
                        *token = state.text[if prepend {
                            k - 1 - j
                        } else {
                            state.text.len() - k + j
                        }];
                    }
                    let moves = matching_moves(index.candidates(&key), k, true);
                    if !moves.is_empty() {
                        return moves;
                    }
                }
            }
            Lookup::Dense(index) => {
                let mut node = if prepend {
                    index.suffix((0..limit).rev().map(|j| state.text[j]))
                } else {
                    index.suffix(state.text.iter().skip(state.text.len() - limit).copied())
                };
                while index.depth(node) >= minimum {
                    let k = index.depth(node);
                    let moves = matching_moves(index.candidates(node), k, false);
                    if !moves.is_empty() {
                        return moves;
                    }
                    node = index.shorter(node);
                }
            }
        }
        dead.set(true);
        vec![]
    }

    fn compatible(&self, moves: &[Move]) -> bool {
        let mut longest: &[u32] = &[];
        for m in moves {
            let w = &self.words[m.index];
            let extension = if m.prepend {
                &w[..w.len() - m.overlap]
            } else {
                &w[m.overlap..]
            };
            let n = longest.len().min(extension.len());
            let agrees = if m.prepend {
                longest[longest.len() - n..] == extension[extension.len() - n..]
            } else {
                longest[..n] == extension[..n]
            };
            if !agrees {
                return false;
            }
            if extension.len() > longest.len() {
                longest = extension;
            }
        }
        true
    }

    fn step(&self, state: &State) -> Step {
        let append = self.boundary(state, false);
        let prepend = self.boundary(state, true);
        // Duplicates at an unchanged boundary are safe to account for first.
        for m in append.iter().chain(&prepend) {
            let boundary = if m.prepend {
                state.min_pos
            } else {
                state.max_pos
            };
            if self.words[m.index].len() == m.overlap && self.fragments[m.index].pos == boundary {
                return Step::Safe(*m);
            }
        }
        // Place shorter nested evidence before extending past it; source order
        // must not cause a contained fragment to become stranded in the interior.
        let preferred = |moves: &[Move]| {
            self.compatible(moves)
                .then(|| {
                    moves
                        .iter()
                        .min_by_key(|m| {
                            let pos = self.fragments[m.index].pos;
                            (
                                if m.prepend { u8::MAX - pos } else { pos },
                                self.words[m.index].len() - m.overlap,
                                m.index,
                            )
                        })
                        .copied()
                })
                .flatten()
        };
        let a = preferred(&append);
        let p = preferred(&prepend);
        // A fragment that can extend either end needs direction disambiguation.
        if let (Some(a), Some(p)) = (a, p)
            && a.index == p.index
            && self.words[a.index].len() > a.overlap
            && self.words[p.index].len() > p.overlap
        {
            return Step::Branch(vec![a, p]);
        }
        let safe = a
            .into_iter()
            .chain(p)
            .min_by_key(|m| (usize::MAX - m.overlap, m.index, m.prepend));
        if let Some(m) = safe {
            return Step::Safe(m);
        }
        let moves = append.into_iter().chain(prepend).collect::<Vec<_>>();
        if moves.is_empty() {
            Step::End
        } else {
            Step::Branch(moves)
        }
    }

    /// Enumerate alternatives admitted by the positional/max-overlap model.
    /// A pruned search NEVER claims uniqueness, even after finding a completion.
    fn search(&self, initial: &State, budget: &mut usize) -> Search {
        const MAX_BYTES: usize = 64 * 1024 * 1024;
        if *budget == 0 || initial.estimated_bytes() > MAX_BYTES {
            return Search::Exhausted;
        }
        let mut pending = vec![initial.clone()];
        let mut bytes = initial.estimated_bytes();
        let mut complete: Option<State> = None;
        let mut visited = FxHashSet::default();
        let mut visited_bytes = 0;
        while let Some(mut state) = pending.pop() {
            bytes = bytes.saturating_sub(state.estimated_bytes());
            loop {
                let held = complete.as_ref().map_or(0, State::estimated_bytes) + visited_bytes;
                if bytes
                    .saturating_add(state.estimated_bytes())
                    .saturating_add(held)
                    > MAX_BYTES
                {
                    return Search::Exhausted;
                }
                if state.used_count == self.fragments.len() {
                    if let Some(previous) = &complete {
                        if previous.text != state.text {
                            return Search::Multiple;
                        }
                    } else {
                        complete = Some(state);
                    }
                    break;
                }
                if *budget == 0 {
                    return Search::Exhausted;
                }
                *budget -= 1;
                match self.step(&state) {
                    Step::Safe(m) => state.apply(m, &self.words, self.fragments, self.cap),
                    Step::End => break,
                    Step::Branch(moves) => {
                        let key = SearchKey::new(&state);
                        if visited.contains(&key) {
                            break;
                        }
                        let key_bytes = key.estimated_bytes();
                        if bytes
                            .saturating_add(state.estimated_bytes())
                            .saturating_add(held)
                            .saturating_add(key_bytes)
                            > MAX_BYTES
                        {
                            return Search::Exhausted;
                        }
                        visited_bytes += key_bytes;
                        visited.insert(key);
                        // Retain all alternatives; this is not beam pruning.
                        for m in moves {
                            if *budget == 0 {
                                return Search::Exhausted;
                            }
                            *budget -= 1;
                            let estimate = state
                                .estimated_bytes()
                                .saturating_add(self.words[m.index].len() * 4 + 8);
                            let held =
                                complete.as_ref().map_or(0, State::estimated_bytes) + visited_bytes;
                            if pending.len() >= 64
                                || bytes.saturating_add(estimate).saturating_add(held) > MAX_BYTES
                            {
                                return Search::Exhausted;
                            }
                            let mut child = state.clone();
                            child.apply(m, &self.words, self.fragments, self.cap);
                            if bytes
                                .saturating_add(child.estimated_bytes())
                                .saturating_add(held)
                                > MAX_BYTES
                            {
                                return Search::Exhausted;
                            }
                            bytes += child.estimated_bytes();
                            pending.push(child);
                        }
                        break;
                    }
                }
            }
        }
        complete.map_or(Search::NoCompletion, Search::Unique)
    }

    fn assemble(
        &self,
        state: &mut State,
        budget: &mut usize,
    ) -> (&'static str, &'static str, usize) {
        loop {
            match self.step(state) {
                Step::Safe(m) => state.apply(m, &self.words, self.fragments, self.cap),
                Step::End => {
                    let status = if state.used_count == self.fragments.len() {
                        "all_fragments_merged"
                    } else {
                        "no_confident_overlap"
                    };
                    return (status, "not_needed", 0);
                }
                Step::Branch(moves) => {
                    let outcome = if self.options.search_budget == 0 {
                        Search::Exhausted
                    } else {
                        self.search(state, budget)
                    };
                    return match outcome {
                        Search::Unique(solved) => {
                            *state = solved;
                            ("all_fragments_merged", "unique_complete_assembly", 0)
                        }
                        Search::Multiple => (
                            "ambiguous_continuation",
                            "multiple_complete_assemblies",
                            moves.len(),
                        ),
                        Search::NoCompletion => (
                            "ambiguous_continuation",
                            "no_complete_assembly",
                            moves.len(),
                        ),
                        Search::Exhausted => (
                            "ambiguous_continuation",
                            if self.options.search_budget == 0 {
                                "disabled"
                            } else {
                                "budget_exhausted"
                            },
                            moves.len(),
                        ),
                    };
                }
            }
        }
    }

    fn text(&self, state: &State) -> String {
        state
            .text
            .iter()
            .map(|&i| self.tokens[i as usize])
            .collect()
    }
}

/// Text remains one supported assembly; residual pieces are separately labelled.
/// Search uniqueness refers to this model, not publisher-text accuracy.
pub fn reconstruct_type2(mut fragments: Vec<Fragment>, mut options: Type2Options) -> Recovery {
    options.min_overlap = options.min_overlap.max(1);
    for f in &mut fragments {
        crate::normalize_nfc(&mut f.text);
    }
    fragments.sort_by_key(|f| f.pos);
    let mut diagnostics = Type2Diagnostics {
        tokenization: "extended_grapheme_clusters",
        min_overlap: options.min_overlap,
        max_position_gap: options.max_position_gap,
        minimum_overlap_used: None,
        stop_reason: "all_fragments_merged",
        conflicting_candidates: 0,
        search_budget: options.search_budget,
        search_steps: 0,
        search_status: "not_needed",
        primary_fragment_indices: vec![],
        segments: vec![],
        best_effort: None,
    };
    if fragments.is_empty() {
        return Recovery {
            text: String::new(),
            input_fragments: 0,
            merged_fragments: 0,
            unmerged: vec![],
            type2_diagnostics: Some(diagnostics),
        };
    }
    let engine = Engine::new(&fragments, options);
    let mut remaining = options.search_budget;
    let mut primary = State::seed(
        0,
        vec![false; fragments.len()],
        0,
        &engine.words,
        &fragments,
    );
    let (stop, status, conflicts) = engine.assemble(&mut primary, &mut remaining);
    diagnostics.stop_reason = stop;
    diagnostics.search_status = status;
    diagnostics.conflicting_candidates = conflicts;
    diagnostics.minimum_overlap_used = primary.min_overlap;
    diagnostics.primary_fragment_indices = primary.indices.clone();
    let text = engine.text(&primary);
    let merged_fragments = primary.indices.len();
    let unmerged = fragments
        .iter()
        .zip(&primary.used)
        .filter(|(_, used)| !**used)
        .map(|(f, _)| Unmerged {
            pos: f.pos,
            text: f.text.clone(),
        })
        .collect();
    // Reuse assignments and shared tries rather than rebuild per island.
    let mut assigned = primary.used;
    let mut assigned_count = primary.used_count;
    let mut next_seed = 0;
    while let Some(i) = (next_seed..assigned.len()).find(|&i| !assigned[i]) {
        next_seed = i + 1;
        let mut state = State::seed(i, assigned, assigned_count, &engine.words, &fragments);
        while let Step::Safe(m) = engine.step(&state) {
            state.apply(m, &engine.words, &fragments, engine.cap);
        }
        let stop_reason = match engine.step(&state) {
            Step::Branch(_) => "ambiguous_continuation",
            _ => "no_confident_overlap",
        };
        diagnostics.segments.push(RecoveredSegment {
            text: engine.text(&state),
            min_pos: state.min_pos,
            max_pos: state.max_pos,
            fragment_indices: state.indices,
            stop_reason,
        });
        assigned = state.used;
        assigned_count = state.used_count;
    }
    diagnostics.search_steps = options.search_budget - remaining;
    if options.best_effort {
        diagnostics.best_effort = Some(crate::type2_best_effort::assemble_best_effort(
            &text,
            primary.min_pos,
            &diagnostics.segments,
            options.best_effort_min_overlap.max(1),
        ));
    }
    Recovery {
        text,
        input_fragments: fragments.len(),
        merged_fragments,
        unmerged,
        type2_diagnostics: Some(diagnostics),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn official_unicode_17_grapheme_cases_survive_normalization_and_interning() {
        assert_eq!(unicode_segmentation::UNICODE_VERSION, (17, 0, 0));
        let mut checked = 0;
        for (line_number, line) in include_str!("../tests/fixtures/GraphemeBreakTest-17.0.0.txt")
            .lines()
            .enumerate()
        {
            let case = line.split('#').next().unwrap().trim();
            if case.is_empty() {
                continue;
            }
            let mut expected = vec![];
            let mut current = String::new();
            for token in case.split_whitespace() {
                match token {
                    "÷" => {
                        if !current.is_empty() {
                            expected.push(std::mem::take(&mut current));
                        }
                    }
                    "×" => {}
                    hex => {
                        current.push(char::from_u32(u32::from_str_radix(hex, 16).unwrap()).unwrap())
                    }
                }
            }
            assert!(current.is_empty());
            let mut text = expected.concat();
            crate::normalize_nfc(&mut text);
            for cluster in &mut expected {
                crate::normalize_nfc(cluster);
            }
            let fragments = vec![Fragment { pos: 0, text }];
            let engine = Engine::new(&fragments, Type2Options::default());
            let actual: Vec<_> = engine.words[0]
                .iter()
                .map(|&id| engine.tokens[id as usize])
                .collect();
            assert_eq!(
                actual,
                expected,
                "Unicode conformance line {}",
                line_number + 1
            );
            checked += 1;
        }
        assert_eq!(checked, 766);
    }

    #[test]
    fn both_lookup_paths_and_boundary_caches_match_exhaustive_candidates() {
        let mut seed = 678u64;
        let mut next = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            (seed >> 32) as usize
        };
        for trial in 0..200 {
            let mut fragments: Vec<_> = (0..40)
                .map(|_| {
                    let len = 3 + next() % 25;
                    let text = (0..len)
                        .map(|_| char::from(b'a' + (next() % 5) as u8))
                        .collect();
                    Fragment {
                        pos: (next() % 3 * 10) as u8,
                        text,
                    }
                })
                .collect();
            if trial % 10 == 0 {
                // Forces the high-fanout automaton fallback in both directions.
                fragments.extend((0..300).map(|_| Fragment {
                    pos: 0,
                    text: "abcdabcd".into(),
                }));
            }
            fragments.sort_by_key(|f| f.pos);
            let options = Type2Options {
                min_overlap: 1 + trial % 4,
                ..Type2Options::default()
            };
            let engine = Engine::new(&fragments, options);
            let mut state = State::seed(
                0,
                vec![false; fragments.len()],
                0,
                &engine.words,
                &fragments,
            );
            for _ in 0..30 {
                let mut moves = vec![];
                for prepend in [false, true] {
                    let mut expected = vec![];
                    for k in (options.min_overlap..=engine.cap.min(state.text.len())).rev() {
                        let start = if prepend { 0 } else { state.text.len() - k };
                        if !(start..start + k).any(|j| engine.meaningful[state.text[j] as usize]) {
                            continue;
                        }
                        for (i, w) in engine.words.iter().enumerate().skip(1) {
                            let pos = fragments[i].pos;
                            let valid_pos = if prepend {
                                pos <= state.min_pos
                                    && state.min_pos - pos <= options.max_position_gap
                            } else {
                                pos >= state.max_pos
                                    && pos - state.max_pos <= options.max_position_gap
                            };
                            if state.used[i] || !valid_pos || w.len() < k {
                                continue;
                            }
                            if (0..k).all(|j| {
                                w[if prepend { w.len() - k + j } else { j }]
                                    == state.text[start + j]
                            }) {
                                expected.push(Move {
                                    index: i,
                                    overlap: k,
                                    prepend,
                                });
                            }
                        }
                        if !expected.is_empty() {
                            break;
                        }
                    }
                    let actual = engine.boundary(&state, prepend);
                    assert_eq!(actual, expected, "trial {trial}, prepend {prepend}");
                    moves.extend(actual);
                }
                if moves.is_empty() {
                    break;
                }
                state.apply(
                    moves[next() % moves.len()],
                    &engine.words,
                    &fragments,
                    engine.cap,
                );
            }
        }
    }
}
