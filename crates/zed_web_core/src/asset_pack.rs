//! The asset pack: one uncompressed tar (`zed-assets.tar`, BUILD-SPEC 3.5) holding fonts,
//! icons, images, themes and sounds, parsed once at boot into memory. The reader handles the
//! ustar/GNU/pax layout `tar -cf` produces (512-byte headers, octal sizes, `L` long-name
//! entries, `ustar` prefixes, pax `x` headers with a `path` record as bsdtar writes for long
//! names) and nothing else: no compression, no sparse files, no links.

use collections::BTreeMap;

/// Entries beyond this count are refused (defense in depth: the pack is same-origin and
/// content-hashed, but a corrupt one must not exhaust memory).
pub const MAX_ENTRIES: usize = 8192;
/// Total payload bytes beyond this are refused.
pub const MAX_TOTAL_BYTES: u64 = 64 * 1024 * 1024;

const BLOCK: usize = 512;

/// The parsed pack: path (no leading `./`) to contents.
#[derive(Default, Debug)]
pub struct AssetPack {
    files: BTreeMap<String, Vec<u8>>,
}

impl AssetPack {
    /// Parses a tar. Entries are regular files only (directories are skipped); the leading
    /// `./` is stripped. Rejects absolute paths, any `..` component, more than
    /// [`MAX_ENTRIES`] entries or more than [`MAX_TOTAL_BYTES`] of payload.
    pub fn from_tar(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut files = BTreeMap::default();
        let mut offset = 0usize;
        let mut total: u64 = 0;
        let mut pending_long_name: Option<String> = None;

        if bytes.len() < BLOCK {
            anyhow::bail!(
                "asset pack is too short to be a tar ({} bytes)",
                bytes.len()
            );
        }

        while offset + BLOCK <= bytes.len() {
            let header = &bytes[offset..offset + BLOCK];
            if header.iter().all(|&b| b == 0) {
                // End-of-archive marker (two zero blocks; one is enough for us).
                break;
            }
            let magic = &header[257..263];
            if !(magic.starts_with(b"ustar") || magic.iter().all(|&b| b == 0)) {
                anyhow::bail!("asset pack entry at {offset} is not a tar header");
            }
            if !checksum_ok(header) {
                anyhow::bail!("asset pack entry at {offset} has a bad header checksum");
            }

            let size = parse_octal(&header[124..136])
                .ok_or_else(|| anyhow::anyhow!("asset pack entry at {offset} has a bad size"))?;
            let type_flag = header[156];
            let data_start = offset + BLOCK;
            let data_end = data_start
                .checked_add(size as usize)
                .filter(|end| *end <= bytes.len())
                .ok_or_else(|| anyhow::anyhow!("asset pack entry at {offset} is truncated"))?;
            let data = &bytes[data_start..data_end];
            // Advance to the next header (data is padded to a whole block).
            offset = data_start + (size as usize).div_ceil(BLOCK) * BLOCK;

            match type_flag {
                // GNU long name: the data is the next entry's name.
                b'L' => {
                    pending_long_name = Some(nul_terminated(data).to_string());
                    continue;
                }
                // pax extended header: its `path` record (when present) names the next
                // entry; other records (mtime, size overrides we never need) are ignored.
                b'x' => {
                    if let Some(path) = pax_record(data, "path") {
                        pending_long_name = Some(path);
                    }
                    continue;
                }
                // Regular file (ustar `0`, old-style NUL).
                b'0' | 0 => {}
                // Directories, links, pax global headers, sparse files: skipped.
                _ => {
                    pending_long_name = None;
                    continue;
                }
            }

            let name = match pending_long_name.take() {
                Some(name) => name,
                None => {
                    let name = nul_terminated(&header[0..100]);
                    let prefix = if magic.starts_with(b"ustar") {
                        nul_terminated(&header[345..500])
                    } else {
                        ""
                    };
                    if prefix.is_empty() {
                        name.to_string()
                    } else {
                        format!("{prefix}/{name}")
                    }
                }
            };
            let path = normalize_path(&name)?;

            total += size;
            if total > MAX_TOTAL_BYTES {
                anyhow::bail!("asset pack exceeds {MAX_TOTAL_BYTES} bytes of payload (at {path})");
            }
            files.insert(path, data.to_vec());
            if files.len() > MAX_ENTRIES {
                anyhow::bail!("asset pack has more than {MAX_ENTRIES} entries");
            }
        }

        Ok(Self { files })
    }

    /// The bytes of `path` (no leading `./`), if the pack has it.
    pub fn get(&self, path: &str) -> Option<&[u8]> {
        self.files.get(path).map(Vec::as_slice)
    }

    /// Every path starting with `prefix`, in sorted order.
    pub fn list(&self, prefix: &str) -> Vec<String> {
        self.files
            .keys()
            .filter(|path| path.starts_with(prefix))
            .cloned()
            .collect()
    }

    /// Number of files in the pack.
    pub fn len(&self) -> usize {
        self.files.len()
    }

    /// Whether the pack holds no files.
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

fn nul_terminated(bytes: &[u8]) -> &str {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end]).unwrap_or("")
}

/// The value of `key` in a pax extended header body: a sequence of `"<len> <key>=<value>\n"`
/// records where `len` counts the whole record including the length field and the newline.
/// The last record for `key` wins, as in the pax specification; a malformed record ends the
/// scan.
fn pax_record(data: &[u8], key: &str) -> Option<String> {
    let mut found = None;
    let mut rest = data;
    while !rest.is_empty() {
        let Some(space) = rest.iter().position(|&b| b == b' ') else {
            break;
        };
        let Some(len) = std::str::from_utf8(&rest[..space])
            .ok()
            .and_then(|len| len.parse::<usize>().ok())
        else {
            break;
        };
        if len <= space + 1 || len > rest.len() {
            break;
        }
        let record = &rest[space + 1..len];
        let record = record.strip_suffix(b"\n").unwrap_or(record);
        if let Some(value) = record
            .strip_prefix(key.as_bytes())
            .and_then(|value| value.strip_prefix(b"="))
        {
            found = std::str::from_utf8(value).ok().map(str::to_string);
        }
        rest = &rest[len..];
    }
    found
}

fn parse_octal(field: &[u8]) -> Option<u64> {
    let text = nul_terminated(field).trim();
    if text.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(text, 8).ok()
}

fn checksum_ok(header: &[u8]) -> bool {
    let Some(stored) = parse_octal(&header[148..156]) else {
        return false;
    };
    let mut sum: u64 = 0;
    for (i, &b) in header.iter().enumerate() {
        sum += if (148..156).contains(&i) {
            b' ' as u64
        } else {
            b as u64
        };
    }
    sum == stored
}

/// Strips a leading `./`, rejects absolute paths and `..` components, and joins the rest
/// with `/`.
fn normalize_path(name: &str) -> anyhow::Result<String> {
    if name.starts_with('/') || name.starts_with('\\') {
        anyhow::bail!("asset pack entry {name:?} has an absolute path");
    }
    let mut parts = Vec::new();
    for part in name.split('/') {
        match part {
            "" | "." => continue,
            ".." => anyhow::bail!("asset pack entry {name:?} escapes the pack"),
            part => parts.push(part),
        }
    }
    if parts.is_empty() {
        anyhow::bail!("asset pack entry {name:?} has an empty path");
    }
    Ok(parts.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal ustar writer for the tests (regular files and directories only).
    fn tar_entry(name: &str, type_flag: u8, data: &[u8]) -> Vec<u8> {
        let mut header = vec![0u8; BLOCK];
        header[..name.len()].copy_from_slice(name.as_bytes());
        header[100..108].copy_from_slice(b"0000644\0");
        header[124..136].copy_from_slice(format!("{:011o}\0", data.len()).as_bytes());
        header[156] = type_flag;
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        let sum: u64 = header
            .iter()
            .enumerate()
            .map(|(i, &b)| {
                if (148..156).contains(&i) {
                    b' ' as u64
                } else {
                    b as u64
                }
            })
            .sum();
        header[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        let mut out = header;
        out.extend_from_slice(data);
        let pad = (BLOCK - data.len() % BLOCK) % BLOCK;
        out.extend(std::iter::repeat_n(0u8, pad));
        out
    }

    fn tar(entries: &[(&str, u8, &[u8])]) -> Vec<u8> {
        let mut out = Vec::new();
        for (name, flag, data) in entries {
            out.extend(tar_entry(name, *flag, data));
        }
        out.extend(std::iter::repeat_n(0u8, BLOCK * 2));
        out
    }

    #[test]
    fn tar_roundtrip() {
        let bytes = tar(&[
            ("fonts/", b'5', b""),
            ("fonts/lilex/Lilex-Regular.ttf", b'0', b"0123456789abcdef"),
            ("themes/one.json", b'0', b"{}"),
            ("./icons/a.svg", b'0', b"<svg/>"),
        ]);
        let pack = AssetPack::from_tar(&bytes).unwrap();
        assert_eq!(pack.len(), 3);
        assert_eq!(pack.get("icons/a.svg"), Some(&b"<svg/>"[..]));
        assert_eq!(
            pack.list("fonts"),
            vec!["fonts/lilex/Lilex-Regular.ttf".to_string()]
        );
        assert!(pack.list("themes").contains(&"themes/one.json".to_string()));
        assert!(pack.get("fonts/").is_none());
    }

    #[test]
    fn tar_long_names() {
        let long = format!("themes/{}/theme.json", "x".repeat(120));
        let bytes = tar(&[
            ("././@LongLink", b'L', long.as_bytes()),
            (&long[..99], b'0', b"{}"),
        ]);
        let pack = AssetPack::from_tar(&bytes).unwrap();
        assert_eq!(pack.get(&long), Some(&b"{}"[..]));
    }

    /// bsdtar (macOS `tar`) writes a pax `x` header with a `path` record for names longer
    /// than 100 bytes, with the ustar name field truncated.
    /// One pax record: the length field counts itself, the space, the record and the
    /// newline.
    fn pax_record_line(record: &str) -> String {
        let mut len = record.len() + 3;
        while len.to_string().len() + 1 + record.len() + 1 != len {
            len += 1;
        }
        let line = format!("{len} {record}\n");
        assert_eq!(line.len(), len);
        line
    }

    #[test]
    fn tar_pax_long_names() {
        let long = format!("icons/{}/icon.svg", "y".repeat(150));
        let pax = pax_record_line(&format!("path={long}"));
        let bytes = tar(&[
            ("PaxHeader/icon.svg", b'x', pax.as_bytes()),
            (&long[..99], b'0', b"<svg/>"),
            ("themes/one.json", b'0', b"{}"),
        ]);
        let pack = AssetPack::from_tar(&bytes).unwrap();
        assert_eq!(pack.get(&long), Some(&b"<svg/>"[..]));
        assert_eq!(pack.get("themes/one.json"), Some(&b"{}"[..]));
        assert_eq!(pack.len(), 2);
    }

    #[test]
    fn pax_records_parse() {
        let body = format!(
            "{}{}",
            pax_record_line("mtime=1700000000"),
            pax_record_line("path=a/b/c.txt")
        );
        let body = body.as_bytes();
        assert_eq!(pax_record(body, "path").as_deref(), Some("a/b/c.txt"));
        assert_eq!(pax_record(body, "mtime").as_deref(), Some("1700000000"));
        assert_eq!(pax_record(body, "size"), None);
        // A malformed length ends the scan without panicking.
        assert_eq!(pax_record(b"99 path=x\n", "path"), None);
        assert_eq!(pax_record(b"garbage", "path"), None);
    }

    #[test]
    fn tar_rejects_garbage() {
        assert!(AssetPack::from_tar(b"not a tar").is_err());
        let mut junk = vec![b'x'; 2048];
        junk[0] = b'y';
        assert!(AssetPack::from_tar(&junk).is_err());
    }

    #[test]
    fn tar_rejects_traversal() {
        for name in ["../x", "/etc/passwd", "a/../../b"] {
            let bytes = tar(&[(name, b'0', b"x")]);
            let error = AssetPack::from_tar(&bytes).unwrap_err().to_string();
            assert!(error.contains(name), "{name}: {error}");
        }
        // A directory entry is skipped, not rejected.
        let bytes = tar(&[("dir/", b'5', b""), ("dir/f", b'0', b"x")]);
        assert_eq!(AssetPack::from_tar(&bytes).unwrap().len(), 1);
    }

    #[test]
    fn tar_rejects_oversize() {
        let mut out = Vec::new();
        for i in 0..=MAX_ENTRIES {
            out.extend(tar_entry(&format!("f{i}"), b'0', b""));
        }
        out.extend(std::iter::repeat_n(0u8, BLOCK * 2));
        assert!(AssetPack::from_tar(&out).is_err());

        let big = vec![0u8; (MAX_TOTAL_BYTES + 1) as usize];
        let bytes = tar(&[("big", b'0', &big)]);
        assert!(AssetPack::from_tar(&bytes).is_err());
    }
}
