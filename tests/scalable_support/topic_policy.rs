use super::*;
use crate::{
    broker::{ShardLoad, ShardLoadReport},
    metadata::PartitionKey,
};
use tidemq_protocol::topic::{HashRange, ShardRoute};

fn policy() -> TopicScalingPolicy {
    TopicScalingPolicy {
        capacity: WorkCapacity {
            publish_bytes_per_second: 1000,
            publish_messages_per_second: 1000,
            delivery_bytes_per_second: 1000,
            deliveries_per_second: 1000,
            read_requests_per_second: 1000,
            ack_bytes_per_second: 1000,
            acknowledgements_per_second: 1000,
            ack_requests_per_second: 1000,
        },
        sample_interval_ms: 100,
        split_window_ms: 200,
        merge_window_ms: 600,
        cooldown_ms: 200,
        ..Default::default()
    }
}

fn sample() -> Sample {
    let id = Id::new_v7().unwrap();
    let range = HashRange {
        start: 0,
        end: None,
    };
    let partition = PartitionKey {
        topic_id: Id::new_v7().unwrap(),
        partition: 0,
    };
    Sample {
        node: 2,
        instance: Id::new_v7().unwrap(),
        domain: Domain {
            shard: BirthShard {
                route: ShardRoute {
                    id,
                    range,
                    group: 2,
                    authority_epoch: 1,
                },
                partition,
                predecessors: Vec::new(),
                sealed_end: None,
            },
            group: 2,
            generation: 1,
            layout: 1,
            active: true,
            owner_epoch: 1,
        },
        report: ShardLoadReport {
            partition,
            birth: Some((id, range)),
            owner_epoch: 1,
            committed_end: 0,
            published_end: 0,
            protected_backlog_bytes: 0,
            frozen: false,
            sealed: false,
            counters: ShardLoad::default(),
        },
    }
}

fn writes(sample: &mut Sample, balanced: bool) {
    let range = sample.domain.shard.route.range;
    sample.report.counters.publish(range, 0, 40);
    sample
        .report
        .counters
        .publish(range, if balanced { u64::MAX } else { 0 }, 40);
    sample.report.committed_end += 2;
}

#[test]
fn explicit_capacity_and_all_policy_boundaries_are_validated() {
    assert!(TopicScalingPolicy::default().validate().is_err());
    let valid = policy();
    valid.validate().unwrap();
    let mut config = crate::config::Config::default();
    config.runtime.topic_scaling = Some(valid.clone());
    let text = toml::to_string(&config).unwrap();
    let parsed: crate::config::Config = toml::from_str(&text).unwrap();
    assert_eq!(
        parsed.runtime.topic_scaling.unwrap().capacity.values(),
        valid.capacity.values()
    );
    for invalid in [
        TopicScalingPolicy {
            low_per_mille: 700,
            ..valid.clone()
        },
        TopicScalingPolicy {
            split_window_ms: 100,
            ..valid.clone()
        },
        TopicScalingPolicy {
            merge_window_ms: 300,
            ..valid.clone()
        },
        TopicScalingPolicy {
            cooldown_ms: 100,
            ..valid.clone()
        },
        TopicScalingPolicy {
            max_shards: 129,
            ..valid.clone()
        },
        TopicScalingPolicy {
            min_shards: 0,
            ..valid
        },
    ] {
        assert!(invalid.validate().is_err());
    }
    let bytes = (std::mem::size_of::<Window>()
        + std::mem::size_of::<Sample>()
        + std::mem::size_of::<Evaluation>()
        + std::mem::size_of::<Domain>()
        + 256)
        * MAX_DOMAINS;
    assert!(bytes < OBSERVATION_BYTES as usize);
}

#[test]
fn complete_windows_require_useful_hash_load_and_distinguish_archive_and_consumer_pressure() {
    let policy = policy();
    let now = Instant::now();
    for (balanced, archived, expected) in [
        (true, false, Status::SplitReady),
        (false, false, Status::HotKeyOrNarrowRange),
        (true, true, Status::ArchiveBehind),
    ] {
        let mut sample = sample();
        let mut window = Window::new(&sample, &policy, now);
        for tick in 1..=2 {
            writes(&mut sample, balanced);
            let result = window
                .observe(
                    &sample,
                    &policy,
                    now + Duration::from_millis(tick * 100),
                    archived,
                )
                .unwrap();
            if tick == 1 {
                assert!(result.split.is_none());
            } else {
                assert_eq!(result.status, expected);
                assert_eq!(result.split.is_some(), expected == Status::SplitReady);
                if result.split.is_some() {
                    assert_eq!(result.split_share, 500);
                }
            }
        }
    }
    let mut sample = sample();
    let mut window = Window::new(&sample, &policy, now);
    sample.report.counters.deliver(100, 1);
    let result = window
        .observe(&sample, &policy, now + Duration::from_millis(100), false)
        .unwrap();
    assert_eq!(result.status, Status::ConsumerCapacity);
    assert!(result.split.is_none());
}

#[test]
fn bursts_gaps_process_owner_and_layout_changes_rebuild_the_entire_window() {
    let policy = policy();
    let now = Instant::now();
    let mut sample = sample();
    let mut window = Window::new(&sample, &policy, now);
    writes(&mut sample, true);
    window
        .observe(&sample, &policy, now + Duration::from_millis(100), false)
        .unwrap();
    let after_burst = window
        .observe(&sample, &policy, now + Duration::from_millis(200), false)
        .unwrap();
    assert!(after_burst.split.is_none());
    for change in 0..5 {
        let at = now + Duration::from_millis(1000 + change * 1000);
        writes(&mut sample, true);
        assert!(window.observe(&sample, &policy, at, false).is_none());
        writes(&mut sample, true);
        window
            .observe(&sample, &policy, at + Duration::from_millis(100), false)
            .unwrap();
        match change {
            0 => sample.instance = Id::new_v7().unwrap(),
            1 => sample.report.owner_epoch += 1,
            2 => sample.domain.layout += 1,
            3 => sample.domain.generation += 1,
            _ => sample.report.counters = ShardLoad::default(),
        }
        assert!(
            window
                .observe(&sample, &policy, at + Duration::from_millis(200), false)
                .is_none()
        );
    }
    sample.report.frozen = true;
    assert!(!sample.valid());
}

#[test]
fn merging_requires_long_low_load_in_every_dimension_including_historical_reads() {
    let policy = policy();
    let now = Instant::now();
    let sample = sample();
    let mut window = Window::new(&sample, &policy, now);
    for tick in 1..=6 {
        let row = window
            .observe(
                &sample,
                &policy,
                now + Duration::from_millis(tick * 100),
                false,
            )
            .unwrap();
        assert_eq!(row.low_peak.is_some(), tick == 6);
    }
    let history = Domain {
        active: false,
        ..sample.domain.clone()
    };
    let id = history.shard.route.id;
    let scan = Scan {
        domains: vec![history],
        layouts: Vec::new(),
        groups: BTreeMap::new(),
        registry_revision: 1,
    };
    assert!(!placement::merge_group_low(&policy, &scan, &BTreeMap::new(), 2, 3, &[0; 8]).unwrap());
    let mut load = Evaluation {
        rates: [0; 8],
        low_peak: Some([0; 8]),
        split: None,
        split_share: 1000,
        status: Status::Collecting,
    };
    load.low_peak.as_mut().unwrap()[2] = 301;
    let mut rows = BTreeMap::from([(id, load)]);
    assert!(!placement::merge_group_low(&policy, &scan, &rows, 2, 3, &[0; 8]).unwrap());
    rows.get_mut(&id).unwrap().low_peak.as_mut().unwrap()[2] = 200;
    assert!(placement::merge_group_low(&policy, &scan, &rows, 2, 3, &[0; 8]).unwrap());
    assert!(placement::combined(&[u64::MAX; 8], &[1; 8]).is_err());
}

#[test]
fn an_observed_empty_partition_is_distinct_from_missing_or_foreign_birth_state() {
    let mut sample = sample();
    sample.report.birth = None;
    assert!(sample.valid());
    sample.report.committed_end = 1;
    assert!(!sample.valid());
    sample.report.committed_end = 0;
    sample.report.birth = Some((Id::new_v7().unwrap(), sample.domain.shard.route.range));
    assert!(!sample.valid());
    sample.report.birth = None;
    sample.report.counters.invalid = true;
    assert!(!sample.valid());
}
