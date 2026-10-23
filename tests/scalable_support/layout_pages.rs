use super::*;
use prost::Message;

#[test]
fn layout_pages_stay_within_the_control_frame_and_pin_the_revision() {
    let topic = Id::new_v7().unwrap();
    let (layout, shards) = metadata::Topology::initial(topic, &vec![2; 256]).unwrap();
    let mut topology = metadata::Topology::default();
    topology.layouts.insert(topic, layout);
    for shard in shards {
        topology.shards.insert(shard.route.id, shard);
    }
    let mut after = None;
    let mut ids = std::collections::BTreeSet::new();
    let mut pages = 0;
    loop {
        let page = layout_page(&topology, topic, 1, after).unwrap();
        assert!(page.encoded_len() < 12 * 1024);
        assert!(page.active.len().saturating_add(page.history.len()) <= 32);
        for shard in &page.active {
            assert!(ids.insert(Id::from_slice(&shard.shard_id).unwrap()));
        }
        pages += 1;
        if page.next_after_shard.is_empty() {
            break;
        }
        after = Some(Id::from_slice(&page.next_after_shard).unwrap());
    }
    assert_eq!(pages, 8);
    assert_eq!(ids.len(), 256);
    assert!(layout_page(&topology, topic, 2, after).is_err());
}
