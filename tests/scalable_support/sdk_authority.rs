use super::*;

#[test]
fn transfer_values_bind_the_original_request_and_reject_partial_mixed_or_foreign_receipts() {
    let operation = Id::new_v7().unwrap();
    let shard = Id::new_v7().unwrap();
    let request = public::ManageCluster {
        action: public::ClusterAction::TransferShardAuthority.into(),
        shard_id: bytes(shard),
        group_id: 3,
        ..Default::default()
    };
    let wire = public::AuthorityTransferInfo {
        operation_id: bytes(operation),
        shard_id: bytes(shard),
        topic_id: bytes(Id::new_v7().unwrap()),
        source_group: 2,
        target_group: 3,
        stage: 6,
    };
    let reply = public::ClusterControlResult {
        authority_transfer: Some(wire.clone()),
        ..Default::default()
    };
    assert!(matches!(
        receipt(&request, reply.clone(), operation).unwrap(),
        AdminReceipt::ShardTransferred(_)
    ));
    assert!(receipt(&request, reply.clone(), Id::new_v7().unwrap()).is_err());
    let mut mixed = reply.clone();
    mixed.group = Some(public::ClusterGroup {
        group_id: 3,
        generation: 1,
        voters: vec![1],
    });
    assert!(receipt(&request, mixed, operation).is_err());
    let mut partial = wire.clone();
    partial.stage = 4;
    assert!(
        receipt(
            &request,
            public::ClusterControlResult {
                authority_transfer: Some(partial),
                ..Default::default()
            },
            operation
        )
        .is_err()
    );
    let mut invalid = wire;
    invalid.stage = 99;
    assert!(from_wire(invalid.clone()).is_err());
    invalid.stage = 6;
    invalid.target_group = invalid.source_group;
    assert!(from_wire(invalid).is_err());
    let id: AdminOperationId = operation.to_string().parse().unwrap();
    assert_eq!(id.to_string(), operation.to_string());
    assert!("invalid".parse::<ShardId>().is_err());
}
