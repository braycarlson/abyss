use std::collections::HashMap;

use rift::models::match_v5::{EventsTimeLineDto, TimelineDto};

const SKILL_SLOTS_MAX: usize = 18;
const ITEMS_MAX: usize = 40;
const EVENT_SKILL_LEVEL_UP: &str = "SKILL_LEVEL_UP";
const EVENT_ITEM_PURCHASED: &str = "ITEM_PURCHASED";
const EVENT_ITEM_UNDONE: &str = "ITEM_UNDONE";
const SKILL_LETTERS: [&str; 3] = ["Q", "W", "E"];

#[derive(Clone, Debug, Default)]
pub struct RawBuild {
    pub skills: Vec<i32>,
    pub items: Vec<i32>,
}

#[must_use]
pub fn extract_builds(timeline: &TimelineDto) -> HashMap<i32, RawBuild> {
    let mut builds: HashMap<i32, RawBuild> = HashMap::new();

    for frame in &timeline.info.frames {
        for event in &frame.events {
            apply_event(&mut builds, event);
        }
    }

    builds
}

fn apply_event(builds: &mut HashMap<i32, RawBuild>, event: &EventsTimeLineDto) {
    let Some(participant_id) = event.participant_id else {
        return;
    };

    match event.r#type.as_str() {
        EVENT_SKILL_LEVEL_UP => {
            if let Some(slot) = event.skill_slot {
                let build = builds.entry(participant_id).or_default();

                if build.skills.len() < SKILL_SLOTS_MAX {
                    build.skills.push(slot);
                }
            }
        }
        EVENT_ITEM_PURCHASED => {
            if let Some(item) = event.item_id {
                let build = builds.entry(participant_id).or_default();

                if build.items.len() < ITEMS_MAX {
                    build.items.push(item);
                }
            }
        }
        EVENT_ITEM_UNDONE => {
            if let Some(item) = event.before_id {
                let build = builds.entry(participant_id).or_default();

                if let Some(index) = build.items.iter().rposition(|&purchased| purchased == item) {
                    build.items.remove(index);
                }
            }
        }
        _ => {}
    }
}

#[must_use]
pub fn skill_priority(skills: &[i32]) -> String {
    let mut counts = [0u32; 3];
    let mut first_seen = [usize::MAX; 3];

    for (index, &slot) in skills.iter().enumerate() {
        let position = match slot {
            1 => 0,
            2 => 1,
            3 => 2,
            _ => continue,
        };

        counts[position] += 1;

        if first_seen[position] == usize::MAX {
            first_seen[position] = index;
        }
    }

    let mut order: Vec<usize> = (0..3).filter(|&position| counts[position] > 0).collect();

    order.sort_by(|&left, &right| {
        counts[right]
            .cmp(&counts[left])
            .then(first_seen[left].cmp(&first_seen[right]))
    });

    order
        .iter()
        .map(|&position| SKILL_LETTERS[position])
        .collect::<Vec<_>>()
        .join(">")
}

#[cfg(test)]
mod tests {
    use super::skill_priority;

    #[test]
    fn priority_ranks_by_points_invested() {
        let skills = [1, 3, 1, 3, 1, 4, 1];

        assert_eq!(skill_priority(&skills), "Q>E");
    }

    #[test]
    fn priority_breaks_ties_by_first_level_up() {
        let skills = [1, 2, 1, 2];

        assert_eq!(skill_priority(&skills), "Q>W");
    }

    #[test]
    fn priority_ignores_ultimate_and_empty() {
        let skills: [i32; 0] = [];

        assert_eq!(skill_priority(&skills), "");

        let ultimate_only = [4, 4, 4];

        assert_eq!(skill_priority(&ultimate_only), "");
    }
}
