use std::{
    fs::File,
    io::{BufReader, Cursor, Read},
    path::PathBuf,
    sync::mpsc,
    thread::spawn,
};

use chrono::{naive::serde::ts_microseconds::deserialize, DateTime, Utc};
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
pub struct CzechAddress {
    #[serde(rename = "Kód ADM")]
    pub adm_code: u32,
    #[serde(rename = "Kód obce")]
    pub town_code: u32,
    #[serde(rename = "Název obce")]
    pub town: String,
    #[serde(rename = "Kód MOMC")]
    pub city_part_code: Option<u64>,
    #[serde(rename = "Název MOMC")]
    pub city_part: Option<String>,
    #[serde(rename = "Kód obvodu Prahy")]
    pub prague_part_code: Option<u64>,
    #[serde(rename = "Název obvodu Prahy")]
    pub prague_part: Option<String>,
    #[serde(rename = "Název části obce")]
    pub town_part: String,
    #[serde(rename = "Kód části obce")]
    pub town_part_code: u32,
    #[serde(rename = "Kód ulice")]
    pub street_code: Option<u32>,
    #[serde(rename = "Název ulice")]
    pub street: Option<String>,
    #[serde(rename = "Typ SO")]
    pub object_type: String,
    #[serde(rename = "Číslo domovní")]
    pub number: u32,
    #[serde(rename = "Číslo orientační")]
    pub orientation_number: Option<u32>,
    #[serde(rename = "Znak čísla orientačního")]
    pub orientation_number_sign: Option<String>,
    #[serde(rename = "PSČ")]
    pub zip_code: u32,
    #[serde(rename = "Souřadnice X")]
    pub location_x: Option<f32>,
    #[serde(rename = "Souřadnice Y")]
    pub location_y: Option<f32>,
    #[serde(rename = "Platí Od", with = "ruian_date_format")]
    pub valid_since: DateTime<Utc>,
}

pub fn parse_addresses_from_csv(path: PathBuf) -> anyhow::Result<Vec<CzechAddress>> {
    let mut addresses = Vec::new();
    let mut zip = zip::ZipArchive::new(File::open(path)?)?;
    for i in 0..zip.len() {
        let csv_file = zip.by_index(i)?;
        let decoder = DecodeReaderBytesBuilder::new()
            .encoding(Some(encoding_rs::WINDOWS_1250))
            .build(csv_file);
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b';')
            .has_headers(true)
            .from_reader(decoder);
        addresses.extend(rdr.deserialize().collect::<Result<Vec<_>, _>>()?);
    }
    Ok(addresses)
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
