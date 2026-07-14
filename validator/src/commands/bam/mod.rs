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
}

const DEFAULT_BAM_URL_SCHEME: &str = "http";
const DEFAULT_BAM_HTTP_PORT: u16 = 50055;
const DEFAULT_BAM_HTTPS_PORT: u16 = 50056;

/// Empty values disable BAM. Missing schemes default to HTTP, while omitted
/// ports default to 50055 for HTTP and 50056 for HTTPS. Explicit ports must be
/// non-zero and non-empty.
pub fn extract_bam_url(matches: &ArgMatches) -> Result<Option<String>, BamUrlError> {
    matches
        .value_of("bam_url")
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(|url_str| {
            let parse_target = if url_str.contains("://") {
                url_str.to_owned()
            } else {
                format!("{DEFAULT_BAM_URL_SCHEME}://{url_str}")
            };
            let url =
                Url::parse(&parse_target).map_err(|source| BamUrlError::InvalidUrlFormat {
                    url: url_str.to_owned(),
                    source,
                })?;
            let default_port = if parse_target.starts_with("http://") {
                DEFAULT_BAM_HTTP_PORT
            } else if parse_target.starts_with("https://") {
                DEFAULT_BAM_HTTPS_PORT
            } else {
                return Err(BamUrlError::UnsupportedScheme {
                    scheme: parse_target
                        .split_once("://")
                        .map_or("", |(scheme, _)| scheme)
                        .to_owned(),
                });
            };
            if url.port() == Some(0) {
                return Err(BamUrlError::InvalidUrlFormat {
                    url: url_str.to_owned(),
                    source: ParseError::InvalidPort,
                });
            }

            let authority_start = parse_target.find("://").unwrap() + 3;
            let authority_end = parse_target[authority_start..]
                .find(['/', '?', '#'])
                .map_or(parse_target.len(), |offset| authority_start + offset);
            let authority = &parse_target[authority_start..authority_end];
            let host_port = authority.rsplit('@').next().unwrap();
            let port = if host_port.starts_with('[') {
                host_port
                    .find(']')
                    .and_then(|host_end| host_port[host_end + 1..].strip_prefix(':'))
            } else {
                host_port.rsplit_once(':').map(|(_, port)| port)
            };

            match port {
                Some("") => Err(BamUrlError::InvalidUrlFormat {
                    url: url_str.to_owned(),
                    source: ParseError::InvalidPort,
                }),
                Some(_) => Ok(parse_target),
                None => Ok(format!(
                    "{}:{default_port}{}",
                    &parse_target[..authority_end],
                    &parse_target[authority_end..]
                )),
            }
        })
        .transpose()
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
    if !subcommand_matches.is_present("bam_url") {
        return Ok(());
    }
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

    fn create_test_matches(bam_url: Option<&str>) -> ArgMatches<'_> {
        let app = clap::App::new("test-app").arg(argument());
        let args = bam_url.map_or(vec!["test-app"], |url| vec!["test-app", "--bam-url", url]);
        app.get_matches_from(args)
    }

    // HTTP URLs
    #[test_case("http://localhost", "http://localhost:50055")]
    #[test_case("http://localhost:8080", "http://localhost:8080")]
    #[test_case("http://your-bam.host.wtf:8080", "http://your-bam.host.wtf:8080")]
    #[test_case("http://oh.bam:8080", "http://oh.bam:8080")]
    #[test_case("http://bam:8080/badam", "http://bam:8080/badam")]
    #[test_case("http://bam:8080/dot/slash/", "http://bam:8080/dot/slash/")]
    #[test_case(
        "http://192.168.100.42:8080/ba/da/m",
        "http://192.168.100.42:8080/ba/da/m"
    )]
    #[test_case("http://[::1]", "http://[::1]:50055")]
    #[test_case("http://[fe80::1]:8080/ba/da/m", "http://[fe80::1]:8080/ba/da/m")]
    // HTTPS URLs
    #[test_case("https://bam.example.com", "https://bam.example.com:50056")]
    #[test_case("https://localhost:8081", "https://localhost:8081")]
    #[test_case("https://your-bam.host.wtf:8081", "https://your-bam.host.wtf:8081")]
    #[test_case("https://oh.bam:8081", "https://oh.bam:8081")]
    #[test_case("https://bam:8081/badam", "https://bam:8081/badam")]
    #[test_case(
        "https://192.168.100.42:8081/ba/da/m",
        "https://192.168.100.42:8081/ba/da/m"
    )]
    #[test_case("https://[2001:db8::1]", "https://[2001:db8::1]:50056")]
    #[test_case("https://[fe80::1]:8081/ba/da/m", "https://[fe80::1]:8081/ba/da/m")]
    // Missing schemes default to HTTP
    #[test_case("localhost:8080", "http://localhost:8080")]
    #[test_case("your-bam.host.wtf:8080", "http://your-bam.host.wtf:8080")]
    #[test_case("oh.bam:8080", "http://oh.bam:8080")]
    #[test_case("bam:8080/badam", "http://bam:8080/badam")]
    #[test_case("192.168.100.42:8080/ba/da/m", "http://192.168.100.42:8080/ba/da/m")]
    #[test_case("[fe80::1]:8080/ba/da/m", "http://[fe80::1]:8080/ba/da/m")]
    #[test_case("localhost", "http://localhost:50055")]
    #[test_case("your-bam.host.wtf", "http://your-bam.host.wtf:50055")]
    #[test_case("oh.bam", "http://oh.bam:50055")]
    #[test_case("bam/badam", "http://bam:50055/badam")]
    #[test_case("192.168.100.42/ba/da/m", "http://192.168.100.42:50055/ba/da/m")]
    #[test_case("[fe80::1]/ba/da/m", "http://[fe80::1]:50055/ba/da/m")]
    fn test_extract_bam_url_success(input: &str, expected: &str) {
        let matches = create_test_matches(Some(input));
        assert_eq!(
            extract_bam_url(&matches).unwrap().as_deref(),
            Some(expected)
        );
    }

    // BAM defaults and explicit scheme defaults
    #[test_case("http://bam.example.com", "http://bam.example.com:50055", 50055 ; "http omitted")]
    #[test_case("https://bam.example.com", "https://bam.example.com:50056", 50056 ; "https omitted")]
    #[test_case("http://bam.example.com:80", "http://bam.example.com:80", 80 ; "http explicit")]
    #[test_case("https://bam.example.com:443", "https://bam.example.com:443", 443 ; "https explicit")]
    fn test_extract_bam_url_default_port(input: &str, expected: &str, expected_port: u16) {
        let matches = create_test_matches(Some(input));
        let url = extract_bam_url(&matches).unwrap().unwrap();
        assert_eq!(url, expected);
        assert_eq!(
            Url::parse(&url).unwrap().port_or_known_default(),
            Some(expected_port)
        );
    }

    // Empty inputs disable BAM
    #[test_case("" ; "empty")]
    #[test_case("   " ; "spaces")]
    #[test_case("\t\n " ; "whitespace")]
    fn test_extract_bam_url_empty_inputs(input: &str) {
        let matches = create_test_matches(Some(input));
        assert_eq!(extract_bam_url(&matches).unwrap(), None);
    }

    // Unsupported schemes
    #[test_case("ftp://invalid.com", "ftp")]
    #[test_case("ssh://user@host.com", "ssh")]
    #[test_case("file:///path/to/file", "file")]
    #[test_case("custom://my.host.com", "custom")]
    #[test_case("HTTP://bam.example.com", "HTTP")]
    fn test_extract_bam_url_unsupported_scheme(input: &str, scheme: &str) {
        let matches = create_test_matches(Some(input));
        assert_eq!(
            extract_bam_url(&matches),
            Err(BamUrlError::UnsupportedScheme {
                scheme: scheme.to_owned()
            })
        );
    }

    // Invalid formats
    #[test_case("://invalid", ParseError::RelativeUrlWithoutBase)]
    #[test_case("http://", ParseError::EmptyHost)]
    #[test_case("https://", ParseError::EmptyHost)]
    #[test_case("http://:50055", ParseError::EmptyHost)]
    // Invalid ports
    #[test_case("host.com:abc", ParseError::InvalidPort)]
    #[test_case("host.com:99999", ParseError::InvalidPort)]
    #[test_case("host.com:-1", ParseError::InvalidPort)]
    #[test_case("host.com:0x80", ParseError::InvalidPort)]
    #[test_case("host.com:123x", ParseError::InvalidPort)]
    #[test_case("http://host.com:", ParseError::InvalidPort)]
    #[test_case("https://host.com:", ParseError::InvalidPort)]
    #[test_case("http://host.com:0", ParseError::InvalidPort)]
    #[test_case("https://host.com:0", ParseError::InvalidPort)]
    // Invalid IPv6 addresses
    #[test_case("[::1", ParseError::InvalidIpv6Address)]
    #[test_case("[fe80::1", ParseError::InvalidIpv6Address)]
    #[test_case("[2001:db8::1", ParseError::InvalidIpv6Address)]
    // Invalid IPv6 ports
    #[test_case("[fe80::1]:abc", ParseError::InvalidPort)]
    #[test_case("[::1]:99999", ParseError::InvalidPort)]
    fn test_extract_bam_url_invalid_format(input: &str, source: ParseError) {
        let matches = create_test_matches(Some(input));
        assert_eq!(
            extract_bam_url(&matches),
            Err(BamUrlError::InvalidUrlFormat {
                url: input.to_owned(),
                source
            })
        );
    }

    #[test]
    fn test_extract_bam_url_missing_argument() {
        let matches = create_test_matches(None);
        assert_eq!(extract_bam_url(&matches).unwrap(), None);
    }
}
