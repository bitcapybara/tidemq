#![cfg(all(feature = "heap-profiling", unix))]

use std::os::unix::fs::DirBuilderExt;
use std::process::Command;

#[test]
fn actual_binary_requires_startup_opt_in_and_records_live_then_released_allocations() {
    let directory = std::env::temp_dir().join(format!(
        "tidemq-profile-test-{}",
        tidemq_protocol::Id::new_v7().unwrap()
    ));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .unwrap();
    let disabled = Command::new(env!("CARGO_BIN_EXE_tidemq"))
        .args(["allocator-profile-probe", "--output"])
        .arg(&directory)
        .env_remove("_RJEM_MALLOC_CONF")
        .output()
        .unwrap();
    assert!(!disabled.status.success());
    assert!(String::from_utf8_lossy(&disabled.stderr).contains("startup prof:true"));
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
    let captured = Command::new(env!("CARGO_BIN_EXE_tidemq"))
        .args(["allocator-profile-probe", "--output"])
        .arg(&directory)
        .env("_RJEM_MALLOC_CONF", "prof:true,prof_active:false,lg_prof_sample:17,prof_accum:false,prof_final:false,lg_prof_interval:-1,prof_gdump:false")
        .output().unwrap();
    assert!(
        captured.status.success(),
        "{}",
        String::from_utf8_lossy(&captured.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&captured.stdout).unwrap();
    for phase in ["held", "released"] {
        let item = result.get(phase).unwrap();
        assert_eq!(item.get("allocator").unwrap(), "jemalloc");
        assert!(item.get("bytes").unwrap().as_u64().unwrap() <= 8_388_608);
        let path = item.get("path").unwrap().as_str().unwrap();
        let profile = std::fs::read_to_string(path).unwrap();
        assert!(profile.starts_with("heap_v2/"));
        assert!(profile.contains("MAPPED_LIBRARIES:"));
        assert_eq!(profile.contains("8388608"), phase == "held");
    }
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 2);
    std::fs::remove_dir_all(&directory).unwrap();
}
