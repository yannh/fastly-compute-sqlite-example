use serde::de::{self, Unexpected, Visitor};
use serde::{Deserializer};
use rusqlite::{Connection, params, Result};

#[derive(Debug, serde::Deserialize)]
struct Object {
    #[serde(rename(deserialize = "Object Number"))]
    number: String,
    #[serde(rename(deserialize = "Is Highlight"))]
    #[serde(deserialize_with = "bool_from_str")]
    is_highlight: bool,
    #[serde(rename(deserialize = "Is Timeline Work"))]
    #[serde(deserialize_with = "bool_from_str")]
    is_timeline_work: bool,
    #[serde(rename(deserialize = "Is Public Domain"))]
    #[serde(deserialize_with = "bool_from_str")]
    is_public_domain: bool,
    #[serde(rename(deserialize = "Object ID"))]
    id: u32,
    #[serde(rename(deserialize = "Gallery Number"))]
    gallery_number: Option<u32>,
    #[serde(rename(deserialize = "Department"))]
    department: String,
    #[serde(rename(deserialize = "Accession Year"))]
    accession_year: Option<u32>,
    #[serde(rename(deserialize = "Object Name"))]
    name: String,
    #[serde(rename(deserialize = "Title"))]
    title: String,
    #[serde(rename(deserialize = "Culture"))]
    culture: String,
    #[serde(rename(deserialize = "Period"))]
    period: String,
    #[serde(rename(deserialize = "Dynasty"))]
    dynasty: String,
    #[serde(rename(deserialize = "Reign"))]
    reign: String,
    #[serde(rename(deserialize = "Portfolio"))]
    portfolio: String,
    #[serde(rename(deserialize = "Constituent ID"))]
    constituent_id: String,
    #[serde(rename(deserialize = "Artist Role"))]
    artist_role: String,
    #[serde(rename(deserialize = "Artist Prefix"))]
    artist_prefix: String,
    #[serde(rename(deserialize = "Artist Display Name"))]
    artist_display_name: String,
    #[serde(rename(deserialize = "Artist Display Bio"))]
    artist_display_bio: String,
    #[serde(rename(deserialize = "Artist Suffix"))]
    artist_suffix: String,
    #[serde(rename(deserialize = "Artist Alpha Sort"))]
    artist_alpha_sort: String,
    #[serde(rename(deserialize = "Artist Nationality"))]
    artist_nationality: String,
    #[serde(rename(deserialize = "Artist Begin Date"))]
    artist_begin_date: String,
    #[serde(rename(deserialize = "Artist End Date"))]
    artist_end_date: String,
    #[serde(rename(deserialize = "Artist Gender"))]
    artist_gender: String,
    #[serde(rename(deserialize = "Artist ULAN URL"))]
    artist_ulan_url: String,
    #[serde(rename(deserialize = "Artist Wikidata URL"))]
    artist_wikidata_url: String,
    #[serde(rename(deserialize = "Object Date"))]
    date: String,
    #[serde(rename(deserialize = "Object Begin Date"))]
    begin_date: String,
    #[serde(rename(deserialize = "Object End Date"))]
    end_date: String,
    #[serde(rename(deserialize = "Medium"))]
    medium: String,
    #[serde(rename(deserialize = "Dimensions"))]
    dimensions: String,
    #[serde(rename(deserialize = "Credit Line"))]
    credit_line: String,
    #[serde(rename(deserialize = "Geography Type"))]
    geography_type: String,
    #[serde(rename(deserialize = "City"))]
    city: String,
    #[serde(rename(deserialize = "State"))]
    state: String,
    #[serde(rename(deserialize = "County"))]
    county: String,
    #[serde(rename(deserialize = "Country"))]
    country: String,
    #[serde(rename(deserialize = "Region"))]
    region: String,
    #[serde(rename(deserialize = "Subregion"))]
    subregion: String,
    #[serde(rename(deserialize = "Locale"))]
    locale: String,
    #[serde(rename(deserialize = "Locus"))]
    locus: String,
    #[serde(rename(deserialize = "Excavation"))]
    excavation: String,
    #[serde(rename(deserialize = "River"))]
    river: String,
    #[serde(rename(deserialize = "Classification"))]
    classification: String,
    #[serde(rename(deserialize = "Rights and Reproduction"))]
    rights_and_reproduction: String,
    #[serde(rename(deserialize = "Link Resource"))]
    link_resource: String,
    #[serde(rename(deserialize = "Object Wikidata URL"))]
    wikidata_url: String,
    #[serde(rename(deserialize = "Metadata Date"))]
    metadata_date: String,
    #[serde(rename(deserialize = "Repository"))]
    repository: String,
    #[serde(rename(deserialize = "Tags"))]
    tags: String,
    #[serde(rename(deserialize = "Tags AAT URL"))]
    tags_aat_url: String,
    #[serde(rename(deserialize = "Tags Wikidata URL"))]
    tags_wikidata_url: String,
}

// Bools in the dataset are capitalised
fn bool_from_str<'de, D>(deserializer: D) -> Result<bool, D::Error>
    where
        D: Deserializer<'de>,
{
    struct BoolVisitor;
    impl Visitor<'_> for BoolVisitor {
        type Value = bool;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(
                formatter,
                "truthy (true, True) or falsey (false, False) string"
            )
        }
        fn visit_str<E: de::Error>(self, s: &str) -> Result<bool, E> {
            match s {
                "true" | "True" => {
                    Ok(true)
                }
                "false" | "False" => {
                    Ok(false)
                }
                other => Err(de::Error::invalid_value(Unexpected::Str(other), &self)),
            }
        }
    }

    deserializer.deserialize_str(BoolVisitor)
}


fn main() {
    let obj_path = "/home/yann/fastly-compute-sqlite-example/assets/MetObjects.csv";
    let mut met_objects = csv::Reader::from_path(obj_path).unwrap();
    let conn = Connection::open("met_objects.db").unwrap();
    let _ = conn.execute(
        "create table if not exists objects (
             id INTEGER,
             number TEXT not null,
             is_highlight BOOL not null,
             is_timeline_work BOOL not null,
             is_public_domain BOOL not null,
             gallery_number INTEGER,
             department TEXT,
             accession_year TEXT,
             name TEXT,
             title TEXT,
             culture TEXT,
             period TEXT,
             dynasty TEXT,
             reign TEXT,
             portfolio TEXT,
             constituent_id TEXT,
             artist_role TEXT,
             artist_prefix TEXT,
             artist_display_name TEXT,
             artist_display_bio TEXT,
             artist_suffix TEXT,
             artist_alpha_sort TEXT,
             artist_nationality TEXT,
             artist_begin_date TEXT,
             artist_end_date TEXT,
             artist_gender TEXT,
             artist_ulan_url TEXT,
             artist_wikidata_url TEXT,
             date TEXT,
             begin_date TEXT,
             end_date TEXT,
             medium TEXT,
             dimensions TEXT,
             credit_line TEXT,
             geography_type TEXT,
             city TEXT,
             state TEXT,
             county TEXT,
             country TEXT,
             region TEXT,
             subregion TEXT,
             locale TEXT,
             locus TEXT,
             excavation TEXT,
             river TEXT,
             classification TEXT,
             rights_and_reproduction TEXT,
             link_resource TEXT,
             wikidata_url TEXT,
             metadata_date TEXT,
             repository TEXT,
             tags TEXT,
             tags_aat_url TEXT,
             tags_wikidata_url TEXT
         )",
        params![],
    ).unwrap();


    let mut i = 0;
    for rec in met_objects.deserialize() {
        i = i+1;
        let object:Object = rec.unwrap();
        conn.execute(
            "INSERT INTO objects (
                id,
                number,
                is_highlight,
                is_timeline_work,
                is_public_domain,
                gallery_number,
                department,
                accession_year,
                name,
                title,
                culture,
                period,
                dynasty,
                reign,
                portfolio,
                constituent_id,
                artist_role,
                artist_prefix,
                artist_display_name,
                artist_display_bio,
                artist_suffix,
                artist_alpha_sort,
                artist_nationality,
                artist_begin_date,
                artist_end_date,
                artist_gender,
                artist_ulan_url,
                artist_wikidata_url,
                date,
                begin_date,
                end_date,
                medium,
                dimensions,
                credit_line,
                geography_type,
                city,
                state,
                county,
                country,
                region,
                subregion,
                locale,
                locus,
                excavation,
                river,
                classification,
                rights_and_reproduction,
                link_resource,
                wikidata_url,
                metadata_date,
                repository,
                tags,
                tags_aat_url,
                tags_wikidata_url) \
             values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23\
             , ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?3, ?36, ?37, ?38, ?39, ?40, ?41, ?42, ?43, ?44, ?45, \
             ?46, ?47, ?48, ?49, ?50, ?51, ?52, ?53, ?54)",
            rusqlite::params![
                object.id,
                object.number,
                object.is_highlight,
                object.is_timeline_work,
                object.is_public_domain,
                object.gallery_number,
                object.department,
                object.accession_year,
                object.name,
                object.title,
                object.culture,
                object.period,
                object.dynasty,
                object.reign,
                object.portfolio,
                object.constituent_id,
                object.artist_role,
                object.artist_prefix,
                object.artist_display_name,
                object.artist_display_bio,
                object.artist_suffix,
                object.artist_alpha_sort,
                object.artist_nationality,
                object.artist_begin_date,
                object.artist_end_date,
                object.artist_gender,
                object.artist_ulan_url,
                object.artist_wikidata_url,
                object.date,
                object.begin_date,
                object.end_date,
                object.medium,
                object.dimensions,
                object.credit_line,
                object.geography_type,
                object.city,
                object.state,
                object.county,
                object.country,
                object.region,
                object.subregion,
                object.locale,
                object.locus,
                object.excavation,
                object.river,
                object.classification,
                object.rights_and_reproduction,
                object.link_resource,
                object.wikidata_url,
                object.metadata_date,
                object.repository,
                object.tags,
                object.tags_aat_url,
                object.tags_wikidata_url
            ],
        ).unwrap();
        if i>=10000 {
            break
        }
    }


}
