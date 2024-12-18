use {
    crate::{admin_rpc_service, cli::DefaultArgs},
    clap::{App, Arg, ArgMatches, SubCommand},
    std::path::Path,
    thiserror::Error,
    url::{ParseError, Url},
};

#[derive(Error, Debug, PartialEq)]
pub enum BamUrlError {
    #[error("BAM Invalid URL format: {url}: {source}")]
    InvalidUrlFormat { url: String, source: ParseError },
    #[error("BAM URL unsupported scheme '{scheme}', only http and https are allowed")]
    UnsupportedScheme { scheme: String },
    #[error("BAM URL host cannot be empty")]
    EmptyHost { url: String },
    #[error("BAM URL failed to set default port")]
    PortSetFailed { url: String },
}

const DEFAULT_BAM_URL_SCHEME: &str = "http";
const DEFAULT_BAM_HTTP_PORT: u16 = 50055;
const DEFAULT_BAM_HTTPS_PORT: u16 = 50056;

/// Normalize and validate BAM URL from a string input.
///
/// Takes a potentially incomplete URL and normalizes it by adding default
/// scheme and port if missing.
///
/// # Default values
/// - HTTP URLs default to port 50055
/// - HTTPS URLs default to port 50056
/// - Missing scheme defaults to HTTP
///
/// # Errors
/// Returns an error if the URL is invalid or uses an unsupported scheme.
fn normalize_bam_url(url_str: &str) -> Result<String, BamUrlError> {
    // If empty, return empty string to disable BAM
    if url_str.trim().is_empty() {
        return Ok(String::new());
    }

    let url_str_to_parse = if url_str.contains("://") {
        url_str.into()
    } else {
        format!("{DEFAULT_BAM_URL_SCHEME}://{url_str}")
    };
    let url = Url::parse(&url_str_to_parse).map_err(|e| BamUrlError::InvalidUrlFormat {
        url: url_str.to_string(),
        source: e,
    })?;

    let scheme = url.scheme();
    if !matches!(scheme, "http" | "https") {
        return Err(BamUrlError::UnsupportedScheme {
            scheme: scheme.to_string(),
        });
    }

    // Check if host is empty
    match url.host_str() {
        None | Some("") => {
            return Err(BamUrlError::EmptyHost {
                url: url_str.to_string(),
            })
        }
        Some(_) => {}
    }

    // Transform URL to add default port if missing
    let final_url = match url.port() {
        Some(_) => url, // Port already specified
        None => {
            let default_port = match scheme {
                "https" => DEFAULT_BAM_HTTPS_PORT,
                _ => DEFAULT_BAM_HTTP_PORT,
            };
            let mut url_with_port = url;
            url_with_port
                .set_port(Some(default_port))
                .map_err(|_| BamUrlError::PortSetFailed {
                    url: url_str.to_string(),
                })?;
            url_with_port
        }
    };

    let final_url_string = final_url.to_string();
    if url_str.ends_with('/') {
        Ok(final_url_string)
    } else {
        Ok(final_url_string.trim_end_matches('/').to_string())
    }
}

/// Extract and validate BAM URL from command line arguments.
///
/// This function extracts the BAM URL from clap command line arguments and
/// normalizes it using [`normalize_bam_url`]. If no BAM URL is provided,
/// returns None to disable BAM functionality.
///
/// # Arguments
/// * `matches` - The clap ArgMatches containing parsed command line arguments
///
/// # Returns
/// An optional normalized BAM URL string with appropriate scheme and default port.
/// Returns None if no BAM URL is provided.
///
/// # Errors
/// Returns an error if a BAM URL is provided but the normalization fails
/// (invalid format, unsupported scheme, etc.)
///
/// # Example
/// ```ignore
/// let matches = app.get_matches();
/// let bam_url = extract_bam_url(&matches)?;
/// ```
pub fn extract_bam_url(matches: &ArgMatches) -> Result<Option<String>, BamUrlError> {
    match matches.value_of("bam_url") {
        Some(url) => normalize_bam_url(url).map(Some),
        None => Ok(None),
    }
}

pub fn argument() -> Arg<'static, 'static> {
    Arg::with_name("bam_url")
        .long("bam-url")
        .min_values(0)
        .max_values(1)
        .help("URL of BAM Node; leave empty to disable BAM.")
        .takes_value(true)
}

pub fn command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-bam-config")
        .about("Set configuration for connection to a BAM node")
        .arg(argument())
}

pub fn execute(subcommand_matches: &ArgMatches, ledger_path: &Path) -> crate::commands::Result<()> {
    let bam_url = extract_bam_url(subcommand_matches)
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
    let admin_client = admin_rpc_service::connect(ledger_path);
    admin_rpc_service::runtime()
        .block_on(async move { admin_client.await?.set_bam_url(bam_url).await })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use {super::*, test_case::test_case};

    fn create_test_matches(bam_url: Option<&str>) -> ArgMatches {
        let app = clap::App::new("test-app").arg(argument());

        let args = if let Some(url) = bam_url {
            vec!["test-app", "--bam-url", url]
        } else {
            vec!["test-app"]
        };

        app.get_matches_from(args)
    }

    fn assert_eq_extract_bam_url<S: AsRef<str>, E: AsRef<str>>(input: S, expected: E) {
        let matches = create_test_matches(Some(input.as_ref()));
        let result = extract_bam_url(&matches);
        assert_eq!(
            result.unwrap().unwrap(),
            expected.as_ref(),
            "Failed for input: '{}', expected: '{}'",
            input.as_ref(),
            expected.as_ref(),
        );
    }

    fn assert_extract_bam_url_error<S: AsRef<str>>(input: S, expected_variant: BamUrlError) {
        let matches = create_test_matches(Some(input.as_ref()));
        let result = extract_bam_url(&matches);
        assert!(
            result.is_err(),
            "Expected error for input: '{}', but got: {:?}",
            input.as_ref(),
            result.ok()
        );
        let actual_error = result.unwrap_err();

        // Use matches! macro to check the error variant type
        let matches_expected = matches!(
            (&actual_error, &expected_variant),
            (
                BamUrlError::UnsupportedScheme { .. },
                BamUrlError::UnsupportedScheme { .. }
            ) | (BamUrlError::EmptyHost { .. }, BamUrlError::EmptyHost { .. })
                | (
                    BamUrlError::InvalidUrlFormat { .. },
                    BamUrlError::InvalidUrlFormat { .. }
                )
                | (
                    BamUrlError::PortSetFailed { .. },
                    BamUrlError::PortSetFailed { .. }
                )
        );

        assert!(
            matches_expected,
            "Expected error variant '{:?}' for input: '{}', but got: '{:?}'",
            expected_variant,
            input.as_ref(),
            actual_error,
        );
    }

    // HTTP with port
    #[test_case("http://localhost:8080", "http://localhost:8080" ; "http localhost with port")]
    #[test_case("http://your-bam.host.wtf:8080", "http://your-bam.host.wtf:8080" ; "http domain with port")]
    #[test_case("http://oh.bam:8080", "http://oh.bam:8080" ; "http short domain with port")]
    #[test_case("http://bam:8080/badam", "http://bam:8080/badam" ; "http with port and path")]
    #[test_case("http://bam:8080/dot/slash/", "http://bam:8080/dot/slash/" ; "http with port and path preserving slash")]
    #[test_case("http://192.168.100.42:8080/ba/da/m", "http://192.168.100.42:8080/ba/da/m" ; "http ipv4 with port and path")]
    #[test_case("http://[fe80::1]:8080/ba/da/m", "http://[fe80::1]:8080/ba/da/m" ; "http ipv6 with port and path")]
    // HTTPS with port
    #[test_case("https://localhost:8081", "https://localhost:8081" ; "https localhost with port")]
    #[test_case("https://your-bam.host.wtf:8081", "https://your-bam.host.wtf:8081" ; "https domain with port")]
    #[test_case("https://oh.bam:8081", "https://oh.bam:8081" ; "https short domain with port")]
    #[test_case("https://bam:8081/badam", "https://bam:8081/badam" ; "https with port and path")]
    #[test_case("https://192.168.100.42:8081/ba/da/m", "https://192.168.100.42:8081/ba/da/m" ; "https ipv4 with port and path")]
    #[test_case("https://[fe80::1]:8081/ba/da/m", "https://[fe80::1]:8081/ba/da/m" ; "https ipv6 with port and path")]
    // No scheme with port (should default to http)
    #[test_case("localhost:8080", "http://localhost:8080" ; "localhost with port defaults to http")]
    #[test_case("your-bam.host.wtf:8080", "http://your-bam.host.wtf:8080" ; "domain with port defaults to http")]
    #[test_case("oh.bam:8080", "http://oh.bam:8080" ; "short domain with port defaults to http")]
    #[test_case("bam:8080/badam", "http://bam:8080/badam" ; "host with port and path defaults to http")]
    #[test_case("192.168.100.42:8080/ba/da/m", "http://192.168.100.42:8080/ba/da/m" ; "ipv4 with port and path defaults to http")]
    #[test_case("[fe80::1]:8080/ba/da/m", "http://[fe80::1]:8080/ba/da/m" ; "ipv6 with port and path defaults to http")]
    // No scheme without port (should default to http with default port 50055)
    #[test_case("localhost", "http://localhost:50055" ; "localhost defaults to http with default port")]
    #[test_case("your-bam.host.wtf", "http://your-bam.host.wtf:50055" ; "domain defaults to http with default port")]
    #[test_case("oh.bam", "http://oh.bam:50055" ; "short domain defaults to http with default port")]
    #[test_case("bam/badam", "http://bam:50055/badam" ; "host with path defaults to http with default port")]
    #[test_case("192.168.100.42/ba/da/m", "http://192.168.100.42:50055/ba/da/m" ; "ipv4 with path defaults to http with default port")]
    #[test_case("[fe80::1]/ba/da/m", "http://[fe80::1]:50055/ba/da/m" ; "ipv6 with path defaults to http with default port")]
    fn test_extract_bam_url_success(input: &str, expected: &str) {
        assert_eq_extract_bam_url(input, expected);
    }

    // Empty inputs
    #[test_case("", ""; "empty string")]
    #[test_case("   ", ""; "spaces only")]
    #[test_case("\t\n ", "" ; "whitespace only")]
    fn test_extract_bam_url_empty_inputs(input: &str, expected: &str) {
        let matches = create_test_matches(Some(input));
        let result = extract_bam_url(&matches);
        assert_eq!(
            result.unwrap(),
            expected.to_string().into(),
            "Failed for input: '{input}', expected: '{expected}'"
        );
    }

    // Unsupported schemes
    #[test_case("ftp://invalid.com", "ftp" ; "ftp scheme")]
    #[test_case("ssh://user@host.com", "ssh" ; "ssh scheme")]
    #[test_case("file:///path/to/file", "file" ; "file scheme")]
    #[test_case("custom://my.host.com", "custom" ; "custom scheme")]
    fn test_extract_bam_url_unsupported_schemes(input: &str, scheme: &str) {
        assert_extract_bam_url_error(
            input,
            BamUrlError::UnsupportedScheme {
                scheme: scheme.to_string(),
            },
        );
    }

    // Invalid formats
    #[test_case("://invalid" ; "relative url without base")]
    #[test_case("http://" ; "http with empty host")]
    #[test_case("https://" ; "https with empty host")]
    #[test_case("http://:50055" ; "http with port but empty host")]
    fn test_extract_bam_url_invalid_formats(input: &str) {
        let expected_source = if input == "://invalid" {
            ParseError::RelativeUrlWithoutBase
        } else {
            ParseError::EmptyHost
        };
        assert_extract_bam_url_error(
            input,
            BamUrlError::InvalidUrlFormat {
                url: input.to_string(),
                source: expected_source,
            },
        );
    }

    // Invalid ports
    #[test_case("host.com:abc" ; "non-numeric port")]
    #[test_case("host.com:99999" ; "port too large")]
    #[test_case("host.com:-1" ; "negative port")]
    #[test_case("host.com:0x80" ; "hexadecimal port")]
    fn test_extract_bam_url_invalid_ports(input: &str) {
        assert_extract_bam_url_error(
            input,
            BamUrlError::InvalidUrlFormat {
                url: input.to_string(),
                source: ParseError::InvalidPort,
            },
        );
    }

    // Invalid IPv6 addresses
    #[test_case("[::1" ; "unclosed ipv6 bracket")]
    #[test_case("[fe80::1" ; "unclosed ipv6 bracket with link local")]
    #[test_case("[2001:db8::1" ; "unclosed ipv6 bracket with global")]
    fn test_extract_bam_url_invalid_ipv6_address(input: &str) {
        assert_extract_bam_url_error(
            input,
            BamUrlError::InvalidUrlFormat {
                url: input.to_string(),
                source: ParseError::InvalidIpv6Address,
            },
        );
    }

    // Invalid IPv6 with port
    #[test_case("[fe80::1]:abc" ; "ipv6 with non-numeric port")]
    #[test_case("[::1]:99999" ; "ipv6 with port too large")]
    fn test_extract_bam_url_invalid_ipv6_port(input: &str) {
        assert_extract_bam_url_error(
            input,
            BamUrlError::InvalidUrlFormat {
                url: input.to_string(),
                source: ParseError::InvalidPort,
            },
        );
    }

    #[test]
    fn test_extract_bam_url_missing_argument() {
        let matches = create_test_matches(None);
        let result = extract_bam_url(&matches);
        assert!(result.is_ok());
        let bam_url = result.unwrap();
        assert!(bam_url.is_none());
    }
}
