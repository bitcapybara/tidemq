use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

struct Backing {
    bytes: Vec<u8>,
    released: Arc<AtomicBool>,
}

impl AsRef<[u8]> for Backing {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for Backing {
    fn drop(&mut self) {
        self.released.store(true, Ordering::Release);
    }
}

#[test]
fn a_small_retained_delivery_releases_its_large_wire_batch() {
    let released = Arc::new(AtomicBool::new(false));
    let backing = Bytes::from_owner(Backing {
        bytes: vec![0x5a; 2 * 1024 * 1024],
        released: released.clone(),
    });
    let delivery = public::Delivery {
        offset: 123,
        key: backing.slice(..4),
        body: backing.slice(4..12),
        delivery_token: backing.slice(12..113),
    };
    drop(backing);
    assert!(!released.load(Ordering::Acquire));
    let retained = compact_delivery(delivery);
    assert!(released.load(Ordering::Acquire));
    assert_eq!(retained.offset, 123);
    assert_eq!(retained.key, vec![0x5a; 4]);
    assert_eq!(retained.body, vec![0x5a; 8]);
    assert_eq!(retained.delivery_token, vec![0x5a; 101]);
}
