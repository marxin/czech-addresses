use std::io::{self, Read, Seek};

use chrono::{DateTime, Utc};
use encoding_rs_io::DecodeReaderBytesBuilder;
use serde::Deserialize;
use thiserror::Error;

mod address_date_format {
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
pub struct Address {
    /// Kód adresního místa vedeného v Informačním systému územní identifikace (ISÚI).
    #[serde(rename = "Kód ADM")]
    pub adm_code: u32,
    /// Kód obce vedené v ISÚI, ze které jsou vypsána všechna adresní místa.
    #[serde(rename = "Kód obce")]
    pub town_code: u32,
    /// Název obce, ze které jsou vypsána všechna adresní místa.
    #[serde(rename = "Název obce")]
    pub town: String,
    /// Kód městského obvodu/městské části, který je vyplněn pouze v případě členěných statutárních měst.
    #[serde(rename = "Kód MOMC")]
    pub city_part_code: Option<u64>,
    /// Název městského obvodu/městské části, který je vyplněn pouze v případě členěných statutárních měst.
    #[serde(rename = "Název MOMC")]
    pub city_part: Option<String>,
    /// Kód obvodu Prahy, který je vyplněn pouze v případě Hlavního města Prahy.
    #[serde(rename = "Kód obvodu Prahy")]
    pub prague_part_code: Option<u64>,
    /// Název obvodu Prahy, který je vyplněn pouze v případě Hlavního města Prahy.
    #[serde(rename = "Název obvodu Prahy")]
    pub prague_part: Option<String>,
    /// Kód části obce v rámci nadřazené obce, ve které je číslován stavební objekt.
    #[serde(rename = "Kód části obce")]
    pub town_part_code: u32,
    /// Název části obce v rámci nadřazené obce, ve které je číslován stavební objekt.
    #[serde(rename = "Název části obce")]
    pub town_part: String,
    /// Kód ulice, která je navázána na adresní místo. Může být vyplněn pouze u obcí, které mají zavedenu uliční síť.
    #[serde(rename = "Kód ulice")]
    pub street_code: Option<u32>,
    /// Název ulice, která je navázána na adresní místo. Může být vyplněn pouze u obcí, které mají zavedenu uliční síť.
    #[serde(rename = "Název ulice")]
    pub street: Option<String>,
    /// Typ stavebního objektu.
    #[serde(rename = "Typ SO")]
    pub object_type: String,
    /// Číslo popisné nebo číslo evidenční, podle rozlišeného typu SO.
    #[serde(rename = "Číslo domovní")]
    pub number: u32,
    /// Číslo orientační, slouží k orientaci v rámci nadřazené ulice.
    #[serde(rename = "Číslo orientační")]
    pub orientation_number: Option<u32>,
    /// Znak čísla orientačního, uveden v případě, že je znak k orientačnímu číslu přidělen.
    #[serde(rename = "Znak čísla orientačního")]
    pub orientation_number_sign: Option<String>,
    /// Poštovní směrovací číslo.
    #[serde(rename = "PSČ")]
    pub zip_code: u32,
    /// Souřadnice X definičního bodu adresního místa v systému S-JTSK (systém jednotné trigonometrické sítě katastrální), uvedené v \[m\].
    #[serde(rename = "Souřadnice X")]
    pub location_x: Option<f32>,
    /// Souřadnice Y definičního bodu adresního místa v systému S-JTSK (systém jednotné trigonometrické sítě katastrální), uvedené v \[m\].
    #[serde(rename = "Souřadnice Y")]
    pub location_y: Option<f32>,
    /// Datum platnosti adresního místa ve tvaru RRRR-MM-DD. Pokud je datum 1. 7. 2011, jedná se o adresní místo vzniklé při úvodní migraci dat.
    #[serde(rename = "Platí Od", with = "address_date_format")]
    pub valid_since: DateTime<Utc>,
}

#[derive(Error, Debug)]
pub enum AddressError {
    #[error("IO error")]
    Io(#[from] io::Error),
    #[error("ZIP error")]
    Zip(#[from] zip::result::ZipError),
    #[error("CSV error")]
    Csv(#[from] csv::Error),
}

/// Parses the Czech Republic addresses provided by [RUIAN](https://nahlizenidokn.cuzk.cz/StahniAdresniMistaRUIAN.aspx) in the ZIP format that contains CSV files.
pub fn parse_addresses_from_csv(reader: impl Read + Seek) -> anyhow::Result<Vec<Address>> {
    let mut addresses = Vec::new();
    let mut zip = zip::ZipArchive::new(reader)?;
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
    use std::{fs::File, path::PathBuf, str::FromStr};

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

        let addresses = parse_addresses_from_csv(File::open(csv_archive_path).unwrap()).unwrap();

        assert!(addresses.len() > 2_000_000);

        let address = addresses.iter().find(|a| a.adm_code == 9382372).unwrap();
        assert_eq!(address.town, "Golčův Jeníkov");
        assert_eq!(address.street, Some("Nám. T. G. Masaryka".to_string()));
        assert_eq!(address.number, 110);
    }
}
