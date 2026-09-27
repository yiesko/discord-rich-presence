use std::collections::HashMap;
use std::path::Path;

#[derive(Clone, Debug)]
pub enum Vdf {
  Str(String),
  Map(HashMap<String, Vdf>),
}

/// Tokenize Valve VDF: quoted strings plus braces. Everything else
/// (whitespace, stray bytes) is skipped — manifests are machine-written,
/// no need for error recovery beyond "unparseable".
///
/// Note: quoted braces (`"{"`) are indistinguishable from structural
/// ones downstream — a manifest *value* containing a bare brace would
/// misparse its entry into a skipped map (fail-closed: the library is
/// dropped, never mis-attributed). Real manifests never contain braces
/// in values, so a sentinel token type is not worth the churn.
fn tokenize(input: &str) -> Vec<String> {
  /// Longest single value kept (real paths/manifest fields are bytes):
  /// longer quoted runs are consumed but dropped, so a many-MB quoted
  /// blob cannot balloon one `String`.
  const MAX_TOKEN_LEN: usize = 64 * 1024;
  let mut tokens = Vec::new();
  let mut chars = input.chars();
  while let Some(char) = chars.next() {
    // Token-count cap enforced mid-stream (see `MAX_VDF_TOKENS`): stop
    // one token PAST the cap so the rejection below can fire — stopping
    // exactly at the cap would make oversized input parse truncated.
    if tokens.len() > MAX_VDF_TOKENS {
      break;
    }
    match char {
      '"' => {
        let mut string = String::new();
        let mut closed = false;
        let mut overlong = false;
        loop {
          match chars.next() {
            None => break,
            Some('"') => {
              closed = true;
              break;
            }
            // VDF escapes (`\"`, `\\`): keep the escaped char literally.
            Some('\\') => {
              if let Some(escaped) = chars.next()
                && !overlong
              {
                string.push(escaped);
              }
            }
            Some(char) => {
              if overlong {
                continue;
              }
              if string.len() >= MAX_TOKEN_LEN {
                overlong = true;
                continue;
              }
              string.push(char);
            }
          }
        }
        // Unterminated quote (truncated/corrupt file): drop the partial
        // token instead of merging the rest of the file into it.
        // Overlong tokens are pushed truncated (not dropped): dropping
        // would shift key/value pairing and risk mis-attribution, while
        // a truncated path/id simply fails closed downstream (`is_dir`
        // misses, AppId lookups miss).
        if closed {
          tokens.push(string);
        }
      }
      '{' => tokens.push("{".to_string()),
      '}' => tokens.push("}".to_string()),
      _ => {}
    }
  }
  tokens
}

/// Parse a token stream into nested maps: `"key" "value"` or
/// `"key" { ... }`. Returns the top-level map; trailing garbage after a
/// complete document is ignored.
///
/// Iterative (explicit stack, depth-capped): the recursive version
/// overflowed the stack on crafted nesting (`"a"{"a"...`), killing the
/// scanning thread from a plain data file.
fn parse_vdf(tokens: &[String]) -> HashMap<String, Vdf> {
  /// Maximum nesting depth (real manifests nest ~3 deep).
  const MAX_DEPTH: usize = 64;
  let mut current = HashMap::new();
  let mut stack: Vec<(HashMap<String, Vdf>, String)> = Vec::new();
  let mut pending: Option<String> = None;
  for token in tokens {
    if token == "}" {
      // Close current frame into its parent; stray `}` ends the parse.
      let Some((mut parent, key)) = stack.pop() else {
        break;
      };
      parent.insert(key, Vdf::Map(current));
      current = parent;
      continue;
    }
    if token == "{" {
      // A `{` needs a pending key; otherwise malformed, skip it.
      let Some(key) = pending.take() else {
        continue;
      };
      if stack.len() >= MAX_DEPTH {
        break; // nesting attack: stop, keep what parsed.
      }
      stack.push((std::mem::take(&mut current), key));
      continue;
    }
    if let Some(key) = pending.take() {
      current.insert(key, Vdf::Str(token.clone()));
    } else {
      pending = Some(token.clone());
    }
  }
  // Unclosed frames (truncated tail): fold back up so the parsed prefix
  // still yields data instead of nothing. A dangling final key without a
  // value is dropped, like before.
  while let Some((mut parent, key)) = stack.pop() {
    parent.insert(key, Vdf::Map(current));
    current = parent;
  }
  current
}

/// Parse a whole VDF document into nested maps.
#[must_use]
pub fn parse_vdf_str(input: &str) -> HashMap<String, Vdf> {
  let tokens = tokenize(input);
  // Token-count cap: a 4MB file of quote pairs could otherwise build a
  // million-entry map. Corrupt/oversized input parses to nothing (the
  // library is skipped, never the daemon).
  if tokens.len() > MAX_VDF_TOKENS {
    return HashMap::new();
  }
  parse_vdf(&tokens)
}

/// Bounds for Steam's own files (see [`read_limited`]): real manifests
/// are tens of KB; anything bigger is corrupt or hostile.
pub(crate) const MAX_FOLDERS_BYTES: u64 = 4 * 1024 * 1024;
pub(crate) const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_VDF_TOKENS: usize = 200_000;

/// Read a small text file with a byte cap. Over-cap or non-UTF8 content
/// is an error the callers already treat as "skip this file". Both the
/// pre-read stat AND the post-read length are checked: a concurrent
/// grow/replace (Steam rewriting manifests, symlink swap) between the
/// two must not bypass the cap.
pub(crate) fn read_limited(path: &Path, limit: u64) -> Result<String, std::io::Error> {
  fn too_large() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::FileTooLarge, "VDF file over size cap")
  }
  let meta = std::fs::metadata(path)?;
  if meta.len() > limit {
    return Err(too_large());
  }
  let bytes = std::fs::read(path)?;
  if bytes.len() as u64 > limit {
    return Err(too_large());
  }
  String::from_utf8(bytes)
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "VDF file not UTF-8"))
}

/// Library paths from a parsed `libraryfolders.vdf`: new format nests them
/// under `"path"`, legacy format stores the path directly as the value.
#[must_use]
pub fn library_paths(doc: &HashMap<String, Vdf>) -> Vec<String> {
  let folders = doc
    .get("libraryfolders")
    .or_else(|| doc.get("LibraryFolders"));
  let inner = match folders {
    Some(Vdf::Map(map)) => map,
    _ => return Vec::new(),
  };
  let mut paths = Vec::new();
  for value in inner.values() {
    match value {
      Vdf::Str(path) => paths.push(path.clone()),
      Vdf::Map(map) => {
        if let Some(Vdf::Str(path)) = map.get("path") {
          paths.push(path.clone());
        }
      }
    }
  }
  paths
}

/// `(appid, installdir)` from a parsed `appmanifest_<id>.acf`.
pub fn manifest_ids(doc: &HashMap<String, Vdf>) -> Option<(String, String)> {
  let state = match doc.get("AppState") {
    Some(Vdf::Map(map)) => map,
    _ => return None,
  };
  let appid = match state.get("appid") {
    Some(Vdf::Str(id)) if !id.trim().is_empty() => id.trim().to_string(),
    _ => return None,
  };
  let installdir = match state.get("installdir") {
    Some(Vdf::Str(dir)) if !dir.trim().is_empty() => dir.trim().to_string(),
    _ => return None,
  };
  Some((appid, installdir))
}
