use super::*;

#[test]
fn placement_windows_visit_all_registered_groups_and_candidates_with_fixed_bounds() {
    let nodes = (1..=50)
        .map(|node| {
            let identity = NodeIdentity {
                certificate_name: format!("node-{node}.local"),
                certificate_pin: [node as u8; 32],
                role: if node == 1 { "combined" } else { "storage" }.into(),
                external_endpoint: None,
                retired: false,
            };
            (
                node,
                RegisteredNode {
                    record: NodeRecord {
                        spec: NodeSpec {
                            node_id: node,
                            zone: format!("zone-{node}"),
                            endpoint: format!("127.0.0.1:{}", 7000 + node).parse().unwrap(),
                            roles: identity.roles().unwrap(),
                            schedulable: true,
                        },
                        generation: 1,
                    },
                    identity,
                },
            )
        })
        .collect();
    let directory = Directory {
        revision: 1,
        membership_index: 0,
        nodes,
        groups: (2..38)
            .map(|group_id| {
                (
                    group_id,
                    DataGroup {
                        group_id,
                        generation: 1,
                        voters: [1].into(),
                    },
                )
            })
            .collect(),
        metadata_voters: [1].into(),
    };
    let registry = Registry {
        directory: Mutex::new(directory),
        update: tokio::sync::Mutex::new(()),
        trust: NodeAuthorizations::new(Vec::new()).unwrap(),
    };
    let mut after = None;
    let mut groups = BTreeSet::new();
    let mut candidates = BTreeSet::<u64>::new();
    for round in 0..50 {
        let page = registry.placement_groups(after, round).unwrap();
        assert!(page.len() <= 16);
        for (group, nodes) in &page {
            assert!(nodes.len() <= 32);
            assert!(!nodes.contains(&1));
            groups.insert(*group);
            candidates.extend(nodes);
        }
        after = page.keys().next_back().copied();
    }
    assert_eq!(groups, (2..38).collect());
    assert_eq!(candidates, (2..=50).collect());
    registry.directory.lock().unwrap().groups.clear();
    assert!(registry.placement_groups(after, 51).unwrap().is_empty());
    let mut directory = registry.directory.lock().unwrap();
    let standby = directory.nodes.get_mut(&2).unwrap();
    standby.identity.role = "metadata".into();
    standby.record.spec.roles = [NodeRole::Metadata].into();
    drop(directory);
    assert_eq!(registry.candidates(1, 1).unwrap(), [1, 2]);
    assert_eq!(registry.metadata_voters().unwrap(), [1].into());
}
