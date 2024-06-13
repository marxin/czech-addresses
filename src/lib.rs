use std::{fs::File, path::PathBuf};

use chrono::{DateTime, Utc};
use encoding_rs_io::DecodeReaderBytesBuilder;
use serde::Deserialize;

mod ruian_date_format {
    use std::str::FromStr;

    use chrono::{DateTime, Utc};
    use serde::{self, de, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut s = String::deserialize(deserializer)?;
        s.push('Z');
        DateTime::<Utc>::from_str(&s).map_err(de::Error::custom)
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
    fn parse_addresses() {
        let csv_archive_path = PathBuf::from_str("20240531_OB_ADR_csv.zip").unwrap();
        if !csv_archive_path.exists() {
            let mut response = reqwest::blocking::get(
                "https://vdp.cuzk.cz/vymenny_format/csv/20240531_OB_ADR_csv.zip",
            )
            .unwrap();

            let mut file = File::create_new(csv_archive_path.clone()).unwrap();
            response.copy_to(&mut file).unwrap();
        }

        let addresses = parse_addresses_from_csv(csv_archive_path).unwrap();

        assert!(addresses.len() > 2_000_000);

        let address = addresses.iter().find(|a| a.adm_code == 9382372).unwrap();
        assert_eq!(address.town, "Golčův Jeníkov");
        assert_eq!(address.street, Some("Nám. T. G. Masaryka".to_string()));
        assert_eq!(address.number, 110);
    }
}
