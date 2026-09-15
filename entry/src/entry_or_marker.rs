//! Entry marker types for the PoH recording pipeline.
//!
//! This module defines `EntryOrMarker`, a producer envelope that allows regular entries, block
//! markers, and internal control events to flow through the same PoH recording channel.
use crate::{block_component::VersionedBlockMarker, entry::Entry};

/// Wraps a regular entry, block metadata marker, or internal producer event.
///
/// The PoH recorder uses this type to stream components through a unified channel to downstream
/// consumers, e.g., broadcast stage. This type is not a ledger or wire-format type.
#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum EntryOrMarker {
    /// Internal, nonserialized request for Standard Broadcast, the sole local header producer, to
    /// materialize the canonical header.
    StartBank,
    /// A regular entry containing transactions and/or ticks
    Entry(Entry),
    /// A block metadata marker (header or footer)
    Marker(VersionedBlockMarker),
}

#[cfg(feature = "dev-context-only-utils")]
impl EntryOrMarker {
    pub fn unwrap_entry(self) -> Entry {
        match self {
            Self::StartBank => panic!("Attempting to unwrap StartBank as entry"),
            Self::Entry(e) => e,
            Self::Marker(marker) => panic!("Attempting to unwrap marker as entry {marker:?}"),
        }
    }
}

/// Converts an Entry into an EntryOrMarker.
impl From<Entry> for EntryOrMarker {
    fn from(entry: Entry) -> Self {
        EntryOrMarker::Entry(entry)
    }
}

/// Converts a VersionedBlockMarker into an EntryOrMarker.
impl From<VersionedBlockMarker> for EntryOrMarker {
    fn from(marker: VersionedBlockMarker) -> Self {
        EntryOrMarker::Marker(marker)
    }
}
