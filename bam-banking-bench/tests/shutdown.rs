use std::{
    process::Command,
    thread::sleep,
    time::{Duration, Instant},
};

#[test]
fn benchmark_exits_after_test_duration() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_bam-banking-bench"))
        .args(["--num-keypairs", "1", "--test-duration", "0"])
        .spawn()
        .expect("failed to start benchmark");
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        if let Some(status) = child.try_wait().expect("failed to poll benchmark") {
            assert!(status.success(), "benchmark exited with {status}");
            return;
        }
        if Instant::now() >= deadline {
            child.kill().expect("failed to kill hung benchmark");
            child.wait().expect("failed to reap hung benchmark");
            panic!("benchmark did not exit within 30 seconds");
        }
        sleep(Duration::from_millis(50));
    }
}
