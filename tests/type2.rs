use gdelt_type1::{Fragment, Record, Type2Options, reconstruct_type2};
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

fn fragment(pos: u8, text: &str) -> Fragment {
    Fragment {
        pos,
        text: text.into(),
    }
}

fn windows(text: &str) -> Vec<Fragment> {
    let units: Vec<_> = text.graphemes(true).collect();
    let mut fragments: Vec<_> = (0..units.len())
        .map(|i| Fragment {
            pos: (i * 10 / units.len() * 10) as u8,
            text: units[i.saturating_sub(6)..(i + 7).min(units.len())].concat(),
        })
        .collect();
    // The source is unordered. Deterministically permute it before assembly.
    for i in 0..fragments.len() {
        let j = (i * 17 + 3) % fragments.len();
        fragments.swap(i, j);
    }
    fragments
}

#[test]
fn reconstructs_known_chinese_japanese_thai_khmer_and_mixed_unicode() {
    for text in [
        "北京今天发布新的天气预报，明天上海有小雨。深圳居民正在公园散步。",
        "東京都で新しい科学博物館が開館しました。研究者は最新技術を紹介します。",
        "ประเทศไทยมีประชาชนหลากหลายวัฒนธรรมและภาษาที่น่าสนใจในภูมิภาคเอเชีย",
        "ប្រទេសកម្ពុជាមានវប្បធម៌និងប្រវត្តិសាស្ត្រដ៏សម្បូរបែបនៅក្នុងតំបន់អាស៊ី",
        "最新消息 Cafe\u{301} 👨‍👩‍👧‍👦团队与🏳️‍🌈记者见面，讨论 2025 年 AI 研究成果。",
    ] {
        let text: String = text.nfc().collect();
        let input = windows(&text);
        let count = input.len();
        let result = reconstruct_type2(input, Type2Options::default());
        assert_eq!(result.text, text);
        assert_eq!(count, result.merged_fragments + result.unmerged.len());
    }
}

#[test]
fn joins_kwic_fields_before_normalizing_without_stripping_spaces_or_slashes() {
    let row = Record {
        date: "2025-01-01".into(),
        url: "https://example.com".into(),
        lang: "ja".into(),
        kind: 2,
        pos: 0,
        pre: " / Cafe".into(),
        ngram: "\u{301}".into(),
        post: " 情報  ".into(),
    };
    assert_eq!(row.fragment(true).text, " / Café 情報  ");
    assert_eq!(row.fragment(false).text, row.fragment(true).text);
}

#[test]
fn refuses_conflicting_equal_score_continuations_and_keeps_all_evidence() {
    let r = reconstruct_type2(
        vec![
            fragment(0, "前文甲乙丙丁"),
            fragment(0, "甲乙丙丁戊己"),
            fragment(0, "甲乙丙丁庚辛"),
        ],
        Type2Options::default(),
    );
    assert_eq!(r.text, "前文甲乙丙丁");
    assert_eq!(r.unmerged.len(), 2);
    assert_eq!(
        r.type2_diagnostics.unwrap().stop_reason,
        "ambiguous_continuation"
    );
}

#[test]
fn equivalent_or_nested_extensions_are_not_conflicts() {
    let r = reconstruct_type2(
        vec![
            fragment(0, "序言甲乙丙丁"),
            fragment(0, "甲乙丙丁戊"),
            fragment(0, "甲乙丙丁戊己"),
        ],
        Type2Options::default(),
    );
    assert_eq!(r.text, "序言甲乙丙丁戊己");
    assert_eq!(r.merged_fragments, 3);
}

#[test]
fn nested_extensions_preserve_evidence_in_either_source_order_and_direction() {
    for (seed, long, short, expected) in [
        (
            "序言甲乙丙丁",
            "甲乙丙丁戊己",
            "甲乙丙丁戊",
            "序言甲乙丙丁戊己",
        ),
        (
            "甲乙丙丁后文",
            "前面更多甲乙丙丁",
            "更多甲乙丙丁",
            "前面更多甲乙丙丁后文",
        ),
    ] {
        for reversed in [false, true] {
            let (a, b) = if reversed {
                (short, long)
            } else {
                (long, short)
            };
            let input = vec![fragment(0, seed), fragment(0, a), fragment(0, b)];
            let r = reconstruct_type2(input.clone(), Type2Options::default());
            assert_eq!(r.text, expected);
            assert_eq!(r.merged_fragments, 3);
            verify_evidence(input, r);
        }
    }
}

#[test]
fn nfc_does_not_conflate_distinct_chinese_scripts_or_compatibility_characters() {
    for (seed, different) in [
        ("前文甲乙丙后", "甲乙丙後尾段"),
        ("前文ＡＢＣＤ", "ABCD尾段"),
    ] {
        let r = reconstruct_type2(
            vec![fragment(0, seed), fragment(0, different)],
            Type2Options::default(),
        );
        assert_eq!(r.text, seed);
        assert_eq!(r.merged_fragments, 1);
    }
}

#[test]
fn preserves_grapheme_boundaries_and_canonical_equivalence() {
    let r = reconstruct_type2(
        vec![
            fragment(0, "新闻a\u{301}👨‍👩‍👧‍👦甲乙"),
            fragment(0, "á👨‍👩‍👧‍👦甲乙后续"),
        ],
        Type2Options::default(),
    );
    assert_eq!(r.text, "新闻á👨‍👩‍👧‍👦甲乙后续");
    assert_eq!(r.type2_diagnostics.unwrap().minimum_overlap_used, Some(4));
    // A byte/scalar implementation would match the mark and fabricate a merge.
    let r = reconstruct_type2(
        vec![fragment(0, "ข่าวก้"), fragment(0, "้ต่อไป")],
        Type2Options {
            min_overlap: 1,
            ..Type2Options::default()
        },
    );
    assert_eq!(r.merged_fragments, 1);
}

#[test]
fn short_punctuation_only_and_far_decile_matches_are_rejected() {
    for fragments in [
        vec![fragment(0, "春夏秋冬"), fragment(0, "冬天将至")],
        vec![fragment(0, "新闻...."), fragment(0, "....结束")],
        vec![fragment(0, "前文甲乙丙丁"), fragment(90, "甲乙丙丁后文")],
    ] {
        let r = reconstruct_type2(fragments, Type2Options::default());
        assert_eq!(r.merged_fragments, 1);
        assert_eq!(
            r.type2_diagnostics.unwrap().stop_reason,
            "no_confident_overlap"
        );
    }
    let r = reconstruct_type2(
        vec![fragment(0, "春夏秋冬"), fragment(0, "冬天将至")],
        Type2Options {
            min_overlap: 1,
            ..Type2Options::default()
        },
    );
    assert_eq!(r.text, "春夏秋冬天将至");
}

#[test]
fn future_deciles_remain_available_after_the_boundary_advances() {
    let r = reconstruct_type2(
        vec![
            fragment(0, "第一甲乙丙丁"),
            fragment(10, "甲乙丙丁戊己庚辛"),
            fragment(20, "戊己庚辛最后"),
        ],
        Type2Options::default(),
    );
    assert_eq!(r.text, "第一甲乙丙丁戊己庚辛最后");
    assert_eq!(r.merged_fragments, 3);
}

#[test]
fn boundary_cache_reopens_when_a_short_seed_grows_into_a_longer_overlap() {
    let r = reconstruct_type2(
        vec![
            fragment(0, "abcdefgh"),
            fragment(0, "efghijklmn"),
            fragment(0, "xyzabcdefghij"),
        ],
        Type2Options::default(),
    );
    assert_eq!(r.text, "xyzabcdefghijklmn");
    assert_eq!(r.merged_fragments, 3);
}

#[test]
fn detects_prepend_conflicts_too() {
    let r = reconstruct_type2(
        vec![
            fragment(0, "甲乙丙丁后文"),
            fragment(0, "前甲乙丙丁"),
            fragment(0, "首甲乙丙丁"),
        ],
        Type2Options::default(),
    );
    assert_eq!(r.text, "甲乙丙丁后文");
    assert_eq!(
        r.type2_diagnostics.unwrap().stop_reason,
        "ambiguous_continuation"
    );
}

#[test]
fn handles_empty_single_and_duplicate_inputs() {
    assert_eq!(
        reconstruct_type2(vec![], Type2Options::default()).input_fragments,
        0
    );
    let r = reconstruct_type2(
        vec![fragment(0, "一"), fragment(0, "一")],
        Type2Options::default(),
    );
    assert_eq!(r.text, "一");
    assert_eq!(r.merged_fragments + r.unmerged.len(), 2);
    let r = reconstruct_type2(
        vec![fragment(0, "甲乙丙丁"), fragment(0, "甲乙丙丁")],
        Type2Options::default(),
    );
    assert_eq!(r.text, "甲乙丙丁");
    assert_eq!(r.merged_fragments, 2);
}

#[test]
fn a_blocked_right_boundary_does_not_discard_a_supported_left_extension() {
    let r = reconstruct_type2(
        vec![
            fragment(0, "甲乙丙丁戊己庚辛"),
            fragment(0, "戊己庚辛壬癸"),
            fragment(0, "戊己庚辛天地"),
            fragment(0, "前序甲乙丙丁"),
        ],
        Type2Options::default(),
    );
    assert_eq!(r.text, "前序甲乙丙丁戊己庚辛");
    assert_eq!(r.merged_fragments, 2);
    assert_eq!(r.type2_diagnostics.unwrap().segments.len(), 2);
}

#[test]
fn search_resolves_a_branch_using_a_required_returning_bridge() {
    let r = reconstruct_type2(
        vec![
            fragment(0, "起点甲乙丙丁"),
            fragment(0, "甲乙丙丁春夏秋冬"),
            fragment(0, "春夏秋冬甲乙丙丁"),
            fragment(10, "甲乙丙丁终点结束"),
        ],
        Type2Options::default(),
    );
    assert_eq!(r.text, "起点甲乙丙丁春夏秋冬甲乙丙丁终点结束");
    assert_eq!(r.merged_fragments, 4);
    assert!(r.unmerged.is_empty());
    assert_eq!(
        r.type2_diagnostics.unwrap().search_status,
        "unique_complete_assembly"
    );
}

#[test]
fn multiple_complete_paths_are_not_reported_as_a_solution() {
    let r = reconstruct_type2(
        vec![
            fragment(0, "起点甲乙丙丁"),
            fragment(0, "甲乙丙丁春夏秋冬"),
            fragment(0, "春夏秋冬甲乙丙丁"),
            fragment(0, "甲乙丙丁东南西北"),
            fragment(0, "东南西北甲乙丙丁"),
            fragment(0, "甲乙丙丁终点结束"),
        ],
        Type2Options::default(),
    );
    assert_eq!(r.text, "起点甲乙丙丁");
    assert_eq!(
        r.type2_diagnostics.unwrap().search_status,
        "multiple_complete_assemblies"
    );
}

#[test]
fn search_budget_exhaustion_never_becomes_a_guessed_completion() {
    let input = vec![
        fragment(0, "起点甲乙丙丁"),
        fragment(0, "甲乙丙丁春夏秋冬"),
        fragment(0, "春夏秋冬甲乙丙丁"),
        fragment(10, "甲乙丙丁终点结束"),
    ];
    let complete = reconstruct_type2(input.clone(), Type2Options::default());
    let steps = complete.type2_diagnostics.unwrap().search_steps;
    assert!(steps > 1);
    // Includes budgets that reach one solution but cannot finish checking rivals.
    for budget in 0..steps {
        let r = reconstruct_type2(
            input.clone(),
            Type2Options {
                search_budget: budget,
                ..Type2Options::default()
            },
        );
        assert_eq!(r.text, "起点甲乙丙丁");
        let d = r.type2_diagnostics.unwrap();
        assert_eq!(
            d.search_status,
            if budget == 0 {
                "disabled"
            } else {
                "budget_exhausted"
            }
        );
        assert!(d.search_steps <= budget);
    }
}

#[test]
fn a_fragment_matching_both_ends_must_not_choose_an_arbitrary_orientation() {
    let input = vec![
        fragment(0, "甲乙丙丁甲乙丙丁"),
        fragment(0, "甲乙丙丁春夏秋冬甲乙丙丁"),
    ];
    let r = reconstruct_type2(input.clone(), Type2Options::default());
    assert_eq!(r.text, "甲乙丙丁甲乙丙丁");
    assert_eq!(
        r.type2_diagnostics.as_ref().unwrap().search_status,
        "multiple_complete_assemblies"
    );
    verify_evidence(input, r);
}

fn verify_evidence(mut input: Vec<Fragment>, r: gdelt_type1::Recovery) {
    input.sort_by_key(|f| f.pos);
    let d = r.type2_diagnostics.unwrap();
    let mut assigned = vec![false; input.len()];
    for i in d.primary_fragment_indices {
        assert!(!assigned[i]);
        assigned[i] = true;
        let normalized: String = input[i].text.nfc().collect();
        assert!(r.text.contains(&normalized));
    }
    for segment in d.segments {
        for i in segment.fragment_indices {
            assert!(!assigned[i]);
            assigned[i] = true;
            let normalized: String = input[i].text.nfc().collect();
            assert!(segment.text.contains(&normalized));
        }
    }
    assert!(assigned.into_iter().all(|used| used));
    assert_eq!(r.merged_fragments + r.unmerged.len(), input.len());
}

#[test]
fn missing_bridges_yield_complete_separate_segments_without_invented_connections() {
    let input = vec![
        fragment(0, "第一段春夏秋冬"),
        fragment(0, "春夏秋冬结束"),
        fragment(50, "另一段甲乙丙丁"),
        fragment(60, "甲乙丙丁结尾"),
    ];
    let r = reconstruct_type2(input.clone(), Type2Options::default());
    assert_eq!(r.text, "第一段春夏秋冬结束");
    assert_eq!(
        r.type2_diagnostics.as_ref().unwrap().segments[0].text,
        "另一段甲乙丙丁结尾"
    );
    verify_evidence(input, r);
}

#[test]
fn randomized_multiscript_sources_and_missing_windows_preserve_every_observation() {
    let mut seed = 42u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed >> 32
    };
    let alphabet = [
        "甲",
        "乙",
        "丙",
        "丁",
        "ก้",
        "क्ष",
        "👨‍👩‍👧‍👦",
        "é",
        "か",
        " ",
        "。",
    ];
    for trial in 0..300 {
        let text = (0..70 + next() % 30)
            .map(|_| alphabet[next() as usize % alphabet.len()])
            .collect::<String>();
        let mut input = windows(&text);
        if trial % 2 == 0 {
            input.retain(|_| next() % 5 != 0);
        }
        let result = reconstruct_type2(
            input.clone(),
            Type2Options {
                search_budget: 500,
                ..Type2Options::default()
            },
        );
        if trial % 2 == 1 {
            assert_eq!(result.text, text, "complete source trial {trial}");
        }
        verify_evidence(input, result);
    }
}

#[test]
fn thousands_of_disconnected_sections_reach_bounded_best_effort_fallback() {
    let input: Vec<_> = (0..2000)
        .map(|i| fragment(0, &format!("part{i:06}-terminal")))
        .collect();
    let r = reconstruct_type2(
        input.clone(),
        Type2Options {
            best_effort: true,
            ..Type2Options::default()
        },
    );
    let best = r
        .type2_diagnostics
        .as_ref()
        .unwrap()
        .best_effort
        .as_ref()
        .unwrap();
    assert_eq!(best.joins.len(), 2000);
    assert!(best.comparison_budget_exhausted);
    assert!(input.iter().all(|f| best.text.contains(&f.text)));
    verify_evidence(input, r);
}
