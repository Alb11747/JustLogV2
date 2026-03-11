use std::path::Path;

use anyhow::Result;
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{BufRead, BufReader};
use walkdir::WalkDir;

use crate::model::CanonicalEvent;
use crate::store::{Store, load_lines_from_text_file};

pub fn import_legacy_logs(store: &Store, source: &Path) -> Result<usize> {
    let mut imported = 0usize;
    for entry in WalkDir::new(source).into_iter().filter_map(Result::ok) {
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        let lines = if path.extension().and_then(|ext| ext.to_str()) == Some("gz") {
            let file = File::open(path)?;
            let decoder = GzDecoder::new(file);
            BufReader::new(decoder)
                .lines()
                .collect::<std::io::Result<Vec<_>>>()?
        } else {
            load_lines_from_text_file(path)?
        };
        for line in lines {
            if let Some(event) = CanonicalEvent::from_raw(&line)? && store.insert_event(&event)? {
                imported += 1;
            }
        }
    }
    Ok(imported)
}
