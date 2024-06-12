use std::{
    fs::File,
    io::{BufReader, Cursor, Read},
    path::PathBuf,
    sync::mpsc,
    thread::spawn,
};

use chrono::{DateTime, Utc};
use encoding_rs_io::DecodeReaderBytesBuilder;
use rayon::iter::{ParallelBridge, ParallelIterator};
use serde::Deserialize;

mod ruian_date_format {
    use std::str::FromStr;

    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut s = String::deserialize(deserializer)?;
        s.push('Z');
        // TODO
        Ok(DateTime::<Utc>::from_str(&s).unwrap())
    }
}

#[derive(Deserialize, Debug)]
pub(crate) struct CzechAddress {
    #[serde(rename = "Kód ADM")]
    pub(crate) adm_code: u32,
    #[serde(rename = "Kód obce")]
    pub(crate) town_code: u32,
    #[serde(rename = "Název obce")]
    pub(crate) town: String,
    #[serde(rename = "Kód MOMC")]
    pub(crate) city_part_code: Option<u64>,
    #[serde(rename = "Název MOMC")]
    pub(crate) city_part: Option<String>,
    #[serde(rename = "Kód obvodu Prahy")]
    pub(crate) prague_part_code: Option<u64>,
    #[serde(rename = "Název obvodu Prahy")]
    pub(crate) prague_part: Option<String>,
    #[serde(rename = "Název části obce")]
    pub(crate) town_part: String,
    #[serde(rename = "Kód části obce")]
    pub(crate) town_part_code: u32,
    #[serde(rename = "Kód ulice")]
    pub(crate) street_code: Option<u32>,
    #[serde(rename = "Název ulice")]
    pub(crate) street: Option<String>,
    #[serde(rename = "Typ SO")]
    pub(crate) object_type: String,
    #[serde(rename = "Číslo domovní")]
    pub(crate) number: u32,
    #[serde(rename = "Číslo orientační")]
    pub(crate) orientation_number: Option<u32>,
    #[serde(rename = "Znak čísla orientačního")]
    pub(crate) orientation_number_sign: Option<String>,
    #[serde(rename = "PSČ")]
    pub(crate) zip_code: u32,
    #[serde(rename = "Souřadnice X")]
    pub(crate) location_x: Option<f32>,
    #[serde(rename = "Souřadnice Y")]
    pub(crate) location_y: Option<f32>,
    #[serde(rename = "Platí Od", with = "ruian_date_format")]
    pub(crate) valid_since: DateTime<Utc>,
}

pub(crate) fn parse_addresses_from_csv(path: PathBuf) -> anyhow::Result<Vec<CzechAddress>> {
    let (tx, rx) = mpsc::channel();

    spawn(move || {
        let mut zip = zip::ZipArchive::new(File::open(path).unwrap()).unwrap();
        let mut zip_files: Vec<_> = (0..zip.len())
            .map(|i| (i, zip.by_index(i).unwrap().size()))
            .collect();
        zip_files.sort_by(|a, b| b.1.cmp(&a.1));

        for (i, _) in zip_files {
            let mut zip_file = zip.by_index(i).unwrap();
            let mut buffer = Vec::new();
            buffer.reserve_exact(zip_file.size() as usize);
            zip_file.read_to_end(&mut buffer).unwrap();
            tx.send(buffer).unwrap();
        }
    });

    Ok(rx
        .into_iter()
        .par_bridge()
        .map(|content| {
            let decoder = DecodeReaderBytesBuilder::new()
                .encoding(Some(encoding_rs::WINDOWS_1250))
                .build(BufReader::with_capacity(1024 * 1024, Cursor::new(content)));
            let mut rdr = csv::ReaderBuilder::new()
                .delimiter(b';')
                .has_headers(true)
                .from_reader(decoder);
            rdr.deserialize().map(|a| a.unwrap()).collect::<Vec<_>>()
        })
        .flatten()
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn parse_all_addresses() {
        let addresses =
            parse_addresses_from_csv(PathBuf::from_str("20240531_OB_ADR_csv.zip").unwrap())
                .unwrap();

        assert!(addresses.len() > 2_000_000);

        for addr in addresses.iter().filter(|a| {
            a.town == "Golčův Jeníkov"
                && a.street == Some("Nám. T. G. Masaryka".to_string())
                && a.number == 110
        }) {
            dbg!(addr);
        }
        panic!();
    }
}
