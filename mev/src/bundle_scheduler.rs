use crate::bundle::Bundle;

pub struct BundleScheduler {
    bundles: Vec<Bundle>,
}

impl BundleScheduler {
    pub fn new() -> BundleScheduler {
        BundleScheduler {
            bundles: Vec::with_capacity(100),
        }
    }

    pub fn schedule_bundles(&mut self, bundles: Vec<Bundle>) {
        for bundle in bundles {
            self.bundles.push(bundle);
        }
    }

    pub fn pop(&mut self) -> Option<Bundle> {
        self.bundles.pop()
    }
}
