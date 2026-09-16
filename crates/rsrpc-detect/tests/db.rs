use rsrpc_detect::db::{
  BUNDLED_DETECTABLE, content_hashes, parse_exclusions, trim_detectable, trim_detectable_value,
};

#[test]
fn bundled_snapshot_parses_and_hashes() {
  let parsed: Vec<serde_json::Value> =
    serde_json::from_str(BUNDLED_DETECTABLE).expect("bundled db");
  assert!(!parsed.is_empty(), "bundled snapshot must not be empty");
  let trimmed = trim_detectable(BUNDLED_DETECTABLE).expect("trims");
  let reparsed: Vec<serde_json::Value> = serde_json::from_str(&trimmed).expect("trimmed parses");
  assert_eq!(reparsed.len(), parsed.len());
}

#[test]
fn content_hashes_ignore_volatile_churn() {
  let body_a = r#"[{"id":"1","name":"Game","executables":[{"name":"game.exe","is_launcher":false,"os":"win32"}],"third_party_skus":[],"aliases":[],"hook":false,"description":"v1"}]"#;
  let body_b = r#"[{"id":"1","name":"Game","executables":[{"name":"game.exe","is_launcher":false,"os":"win32"}],"third_party_skus":[],"aliases":[],"hook":true,"description":"v2"}]"#;
  let parsed_a: Vec<rsrpc_detect::db::DetectableActivity> = serde_json::from_str(body_a).unwrap();
  let parsed_b: Vec<serde_json::Value> =
    serde_json::from_str(&trim_detectable(body_b).unwrap()).unwrap();
  let parsed_b: Vec<rsrpc_detect::db::DetectableActivity> =
    serde_json::from_value(serde_json::Value::Array(parsed_b)).unwrap();
  // Raw bytes differ AND hook/description churned: raw hash moves...
  let (raw_a, canon_a) = content_hashes(body_a, &parsed_a);
  let (raw_b, canon_b) = content_hashes(body_b, &parsed_b);
  assert_ne!(raw_a, raw_b);
  // ...but the scanner-visible projection is identical.
  assert_eq!(canon_a, canon_b);
}

#[test]
fn trim_keeps_only_scanner_fields() {
  let value = trim_detectable_value(
    r#"[{"id":"9","name":"N","hook":true,"aliases":["Alt","  ",""],"executables":[{"name":"n.exe","is_launcher":false,"os":"win32","arguments":"--x","extra":1}],"third_party_skus":[{"distributor":"steam","id":"99"},{"distributor":"","id":"x"}],"description":"drop me"}]"#,
  )
  .expect("trims");
  let entry = &value[0];
  assert_eq!(entry["id"], "9");
  assert_eq!(entry["aliases"], serde_json::json!(["Alt"]));
  assert_eq!(entry["executables"][0]["arguments"], "--x");
  assert!(entry.get("description").is_none());
  assert!(entry.get("extra").is_none());
  // Empty distributor dropped, steam kept.
  assert_eq!(entry["third_party_skus"].as_array().unwrap().len(), 1);
}

#[test]
fn exclusions_match_basename_and_patterns() {
  let exclusions = parse_exclusions(
    r#"{"executables":["crashpad_handler.exe"],"patterns":["vcredist.*\\.exe$"]}"#,
  );
  assert!(exclusions.is_excluded("crashpad_handler.exe"));
  assert!(exclusions.is_excluded("VCREDIST_x64.EXE"));
  assert!(!exclusions.is_excluded("game.exe"));
}

#[test]
fn exclusions_tolerate_garbage() {
  let empty = parse_exclusions("not json{{");
  assert!(!empty.is_excluded("anything.exe"));
  // Invalid regex skipped, valid one kept.
  let partial = parse_exclusions(r#"{"patterns":["([invalid","game\\.exe$"]}"#);
  assert!(partial.is_excluded("game.exe"));
  assert!(!partial.is_excluded("other.exe"));
}
