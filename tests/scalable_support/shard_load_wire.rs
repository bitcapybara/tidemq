use super::*;

#[test]
fn fixed_load_pages_bound_decode_allocation_and_fit_the_control_frame() {
    let id = Id::new_v7().unwrap();
    let report = ShardLoadReport {
        partition: PartitionKey {
            topic_id: id,
            partition: u32::MAX,
        },
        birth: Some((
            id,
            tidemq_protocol::topic::HashRange {
                start: 0,
                end: None,
            },
        )),
        owner_epoch: u64::MAX,
        committed_end: u64::MAX,
        published_end: u64::MAX,
        protected_backlog_bytes: u64::MAX,
        frozen: false,
        sealed: true,
        counters: crate::broker::ShardLoad {
            publish_bytes: u64::MAX,
            publish_messages: u64::MAX,
            delivery_bytes: u64::MAX,
            deliveries: u64::MAX,
            read_requests: u64::MAX,
            ack_bytes: u64::MAX,
            acknowledgements: u64::MAX,
            ack_requests: u64::MAX,
            hash_bins: [u64::MAX; 16],
            invalid: true,
        },
    };
    let page = ShardLoadPage {
        cluster: id,
        node: u64::MAX,
        instance: id,
        group: u64::MAX,
        sampled_unix_ms: u64::MAX,
        reports: std::array::from_fn(|_| Some(report.clone())),
    };
    let bytes = wire::binary(&page).unwrap();
    assert!(bytes.len() < 16 * 1024);
    assert_eq!(
        wire::decode_binary::<ShardLoadPage>(&bytes)
            .unwrap()
            .reports,
        page.reports
    );
    assert!(wire::decode_binary::<LoadRequest>(&[0xff; 1024]).is_err());
}
