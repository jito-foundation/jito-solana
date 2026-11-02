//! Auto-detection of XDP zero copy support, ported from firebam's network auto configuration
//! (`src/app/shared/fd_auto_net.c`).
//!
//! Zero copy support varies by driver and exact kernel version, and regressions are common, so it
//! is only enabled on driver and kernel combinations known to work. Everything else uses copy mode.

use {
    agave_xdp::device::NetworkDevice,
    caps::{
        CapSet,
        Capability::{CAP_BPF, CAP_PERFMON},
    },
    log::info,
    std::{fmt, fs, io},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct KernelVersion {
    major: u32,
    minor: u32,
}

impl KernelVersion {
    const fn new(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }

    /// Parses the major and minor version from a kernel release such as `6.8.0-124-generic`.
    fn parse(release: &str) -> Option<Self> {
        let mut parts = release.trim().split('.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next()?;
        let minor_len = minor
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(minor.len());
        let minor = minor[..minor_len].parse().ok()?;
        Some(Self::new(major, minor))
    }

    fn running() -> io::Result<Self> {
        let release = fs::read_to_string("/proc/sys/kernel/osrelease")?;
        Self::parse(&release).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unrecognized kernel release {:?}", release.trim()),
            )
        })
    }
}

impl fmt::Display for KernelVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// A driver and the inclusive range of kernel versions a feature works on.
struct DriverSupport {
    driver: &'static str,
    min_kernel: KernelVersion,
    /// `None` means no upper bound.
    max_kernel: Option<KernelVersion>,
}

impl DriverSupport {
    fn matches(&self, driver: &str, kernel: KernelVersion) -> bool {
        self.driver == driver
            && self.min_kernel <= kernel
            && self
                .max_kernel
                .is_none_or(|max_kernel| kernel <= max_kernel)
    }
}

/// Where XDP driver mode works. Mirrors firebam's "DRV" feature.
const DRIVER_MODE_SUPPORT: &[DriverSupport] = &[
    DriverSupport {
        driver: "mlx5_core",
        min_kernel: KernelVersion::new(5, 15),
        max_kernel: Some(KernelVersion::new(6, 3)),
    },
    DriverSupport {
        driver: "mlx5_core",
        min_kernel: KernelVersion::new(6, 5),
        max_kernel: None,
    },
    DriverSupport {
        driver: "i40e",
        min_kernel: KernelVersion::new(5, 19),
        max_kernel: None,
    },
];

/// Where XDP zero copy works. Mirrors firebam's "Zero Copy" feature.
///
/// Intel drivers are deliberately left out, as they are in firebam. With zero copy on Intel NICs,
/// TX was observed not to wake up after shreds were queued, delaying shred sends by up to 100 ms.
/// Broadcom `bnxt_en` and the Intel `iavf` VF driver do not implement AF_XDP zero copy
/// (`ndo_xsk_wakeup`) at all.
const ZERO_COPY_SUPPORT: &[DriverSupport] = &[DriverSupport {
    driver: "mlx5_core",
    min_kernel: KernelVersion::new(6, 1),
    max_kernel: None,
}];

/// Zero copy attaches the XDP program in driver mode, so the driver and kernel must support both.
fn driver_supports_zero_copy(driver: &str, kernel: KernelVersion) -> bool {
    [DRIVER_MODE_SUPPORT, ZERO_COPY_SUPPORT]
        .iter()
        .all(|support| support.iter().any(|entry| entry.matches(driver, kernel)))
}

/// Decides whether to use zero copy on `interface`, or on the default route interface when
/// `None`. Used when neither `--xdp-zero-copy` nor `--no-xdp-zero-copy` is passed.
pub(super) fn auto_detect(interface: Option<&str>) -> bool {
    match check_support(interface) {
        Ok(setup) => {
            info!("XDP zero copy auto-enabled: {setup}");
            true
        }
        Err(reason) => {
            info!("XDP zero copy not auto-enabled, using copy mode: {reason}");
            false
        }
    }
}

/// Returns a description of the supported setup, or why zero copy can't be used on it.
fn check_support(interface: Option<&str>) -> Result<String, String> {
    let device = match interface {
        Some(interface) => NetworkDevice::new(interface)
            .map_err(|err| format!("failed to open interface {interface:?}: {err}"))?,
        None => NetworkDevice::new_from_default_route()
            .map_err(|err| format!("failed to select interface from the default route: {err}"))?,
    };
    let interface = device.name();
    let driver = device
        .driver()
        .map_err(|err| format!("{err} (virtual interfaces such as bonds have no driver)"))?;
    let kernel = KernelVersion::running()
        .map_err(|err| format!("failed to read the kernel version: {err}"))?;
    let setup = format!("driver {driver} on kernel {kernel} ({interface})");
    if !driver_supports_zero_copy(&driver, kernel) {
        return Err(format!("{setup} is not known to support it"));
    }
    let permitted = caps::read(None, CapSet::Permitted)
        .map_err(|err| format!("failed to read permitted capabilities: {err}"))?;
    if !permitted.contains(&CAP_BPF) || !permitted.contains(&CAP_PERFMON) {
        return Err(format!(
            "{setup} supports it, but CAP_BPF and CAP_PERFMON are not permitted"
        ));
    }
    Ok(setup)
}

#[cfg(test)]
mod tests {
    use {super::*, test_case::test_case};

    #[test_case("6.8.0-124-generic", Some((6, 8)); "ubuntu")]
    #[test_case("5.14.0-503.23.1.el9_5.x86_64", Some((5, 14)); "rhel")]
    #[test_case("6.15-rc1", Some((6, 15)); "release_candidate")]
    #[test_case("6.1.0\n", Some((6, 1)); "trailing_newline")]
    #[test_case("6", None; "missing_minor")]
    #[test_case("6.x", None; "non_numeric_minor")]
    #[test_case("linux", None; "not_a_version")]
    fn test_kernel_version_parse(release: &str, expected: Option<(u32, u32)>) {
        assert_eq!(
            KernelVersion::parse(release),
            expected.map(|(major, minor)| KernelVersion::new(major, minor)),
            "kernel release {release:?} must parse to its major and minor version"
        );
    }

    #[test_case("mlx5_core", 6, 0, false; "mlx5_before_zero_copy")]
    #[test_case("mlx5_core", 6, 1, true; "mlx5_6_1")]
    #[test_case("mlx5_core", 6, 3, true; "mlx5_6_3")]
    #[test_case("mlx5_core", 6, 4, false; "mlx5_6_4_lacks_driver_mode")]
    #[test_case("mlx5_core", 6, 5, true; "mlx5_6_5")]
    #[test_case("mlx5_core", 7, 0, true; "mlx5_7_0")]
    #[test_case("i40e", 6, 8, false; "i40e")]
    #[test_case("ice", 6, 8, false; "ice")]
    #[test_case("ixgbe", 6, 8, false; "ixgbe")]
    #[test_case("", 6, 8, false; "no_driver")]
    fn test_driver_supports_zero_copy(driver: &str, major: u32, minor: u32, expected: bool) {
        assert_eq!(
            driver_supports_zero_copy(driver, KernelVersion::new(major, minor)),
            expected,
            "zero copy support for {driver:?} on kernel {major}.{minor} must match firebam's \
             driver mode and zero copy support lists"
        );
    }

    #[test]
    fn test_auto_detect_on_virtual_interface_uses_copy_mode() {
        assert!(
            !auto_detect(Some("lo")),
            "an interface without a driver must not auto-enable zero copy"
        );
    }

    #[test]
    fn test_auto_detect_on_missing_interface_uses_copy_mode() {
        assert!(
            !auto_detect(Some("agave-no-such-if")),
            "an interface that cannot be opened must not auto-enable zero copy"
        );
    }
}
