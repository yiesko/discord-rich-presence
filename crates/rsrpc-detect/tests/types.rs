//! Detection provenance: stable log identifiers for journal correlation.

use rsrpc_detect::types::DetectSource;

/// Log strings are stable identifiers for journal correlation.
#[test]
fn detect_source_log_strings() {
  assert_eq!(DetectSource::Automaton.as_str(), "automaton");
  assert_eq!(DetectSource::CwdJoined.as_str(), "cwd-joined");
  assert_eq!(DetectSource::ProtonAutomaton.as_str(), "proton-automaton");
  assert_eq!(DetectSource::SteamAppId.as_str(), "steam-app-id");
  assert_eq!(DetectSource::SteamLibrary.as_str(), "steam-library");
  assert_eq!(DetectSource::ExeStem.as_str(), "exe-stem");
  assert_eq!(DetectSource::Folder.as_str(), "folder");
}
