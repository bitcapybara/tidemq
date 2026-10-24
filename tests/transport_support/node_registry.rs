use super::*;
use std::time::Duration;
use tidemq_protocol::{Id, common::TrafficClass};

#[tokio::test]
async fn replacing_a_node_registry_is_atomic_and_wakes_only_revoked_sessions() {
    let first = NodeGrant {
        certificate_pin: [1; 32],
        certificate_name: "node-1.local".into(),
        cluster_id: Id::random().unwrap(),
        node_id: 1,
        role: "storage".into(),
        allowed_groups: vec![7],
        allowed_classes: vec![TrafficClass::Control],
    };
    let registry = NodeAuthorizations::new(vec![first.clone()]).unwrap();
    let bound = registry.bind(first.clone());
    let mut other = first.clone();
    other.node_id = 2;
    assert!(
        registry
            .replace(vec![first.clone(), other.clone()])
            .is_err()
    );
    assert_eq!(registry.snapshot().as_ref(), std::slice::from_ref(&first));
    other.certificate_pin = [2; 32];
    other.certificate_name = "node-2.local".into();
    registry.replace(vec![first, other.clone()]).unwrap();
    assert!(bound.valid());
    assert!(
        tokio::time::timeout(Duration::from_millis(10), bound.revoked())
            .await
            .is_err()
    );
    registry.replace(vec![other]).unwrap();
    assert!(!bound.valid());
    tokio::time::timeout(Duration::from_secs(1), bound.revoked())
        .await
        .unwrap();
    assert!(NodeAuthorizations::new(vec![bound.grant.clone(); 129]).is_err());
}
