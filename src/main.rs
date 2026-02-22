use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use iceberg::CatalogBuilder;
use iceberg::Catalog;
use iceberg::memory::{MemoryCatalog, MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{Error, Result, TableCreation, TableIdent};

use arrow_array::{BinaryArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};

use hex::FromHex;

#[derive(Debug, Clone)]
struct SdRecord {
    device_id: String,
    ts: i64,
    rssi: i32,
    snr: i32,
    phy: String,
    frame_type: String,
    payload: Vec<u8>,
    mioty_qi_1: Option<f64>,
    mioty_qi_2: Option<i32>,
    mioty_qi_3: Option<i32>,
}

fn new_sd_record() -> SdRecord {
    SdRecord {
        device_id: String::new(),
        ts: 0,
        rssi: 0,
        snr: 0,
        phy: String::new(),
        frame_type: String::new(),
        payload: Vec::new(),
        mioty_qi_1: None,
        mioty_qi_2: None,
        mioty_qi_3: None,
    }
}

fn parse_sd_file(path: &Path) -> Result<Vec<SdRecord>> {
    let file = std::fs::File::open(path)
        .map_err(|e| Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

    let mut reader = Reader::from_reader(std::io::BufReader::new(file));
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut records = Vec::new();
    let mut current_device = String::new();
    let mut current_sd = new_sd_record();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => match e.name().as_ref() {
                b"sdl" => {
                    for attr in e.attributes().flatten() {
                        if attr.key.as_ref() == b"deviceId" {
                            current_device = attr
                                .unescape_value()
                                .map_err(|e| Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?
                                .into_owned();
                        }
                    }
                }
                b"sd" => {
                    for attr in e.attributes().flatten() {
                        let key = attr.key.as_ref();
                        let val = attr
                            .unescape_value()
                            .map_err(|e| Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?
                            .into_owned();

                        match key {
                            b"ts" => {
                                current_sd.ts = val.parse::<i64>().map_err(|e| {
                                    Error::new(iceberg::ErrorKind::DataInvalid, e.to_string())
                                })?
                            }
                            b"rssi" => {
                                current_sd.rssi = val.parse::<i32>().map_err(|e| {
                                    Error::new(iceberg::ErrorKind::DataInvalid, e.to_string())
                                })?
                            }
                            b"snr" => {
                                current_sd.snr = val.parse::<i32>().map_err(|e| {
                                    Error::new(iceberg::ErrorKind::DataInvalid, e.to_string())
                                })?
                            }
                            b"phy" => current_sd.phy = val,
                            b"type" => current_sd.frame_type = val,
                            _ => {}
                        }
                    }
                }
                _ => {}
            },

            Ok(Event::Text(e)) => {
                let text = std::str::from_utf8(e.as_ref())
                    .map_err(|e| Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?
                    .trim();

                if !text.is_empty() {
                    current_sd.payload = Vec::from_hex(text)
                        .map_err(|e| Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;
                }
            }

            Ok(Event::End(ref e)) => {
                if e.name().as_ref() == b"sd" {
                    current_sd.device_id = current_device.clone();
                    records.push(current_sd.clone());
                    current_sd = new_sd_record();
                }
            }

            Ok(Event::Eof) => break,

            Err(e) => {
                return Err(Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!("XML parse error: {}", e),
                ))
            }

            _ => {}
        }

        buf.clear();
    }

    Ok(records)
}

fn build_recordbatch(records: &[SdRecord]) -> Result<RecordBatch> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("device_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("rssi", DataType::Int32, false),
        Field::new("snr", DataType::Int32, false),
        Field::new("phy", DataType::Utf8, false),
        Field::new("frame_type", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("mioty_qi_1", DataType::Float64, true),
        Field::new("mioty_qi_2", DataType::Int32, true),
        Field::new("mioty_qi_3", DataType::Int32, true),
    ]));

    Ok(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    records.iter().map(|r| r.device_id.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(records.iter().map(|r| r.ts).collect::<Vec<_>>())),
                Arc::new(Int32Array::from(records.iter().map(|r| r.rssi).collect::<Vec<_>>())),
                Arc::new(Int32Array::from(records.iter().map(|r| r.snr).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    records.iter().map(|r| r.phy.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    records.iter().map(|r| r.frame_type.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(BinaryArray::from(
                    records.iter().map(|r| r.payload.as_slice()).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(records.iter().map(|r| r.mioty_qi_1).collect::<Vec<_>>())),
                Arc::new(Int32Array::from(records.iter().map(|r| r.mioty_qi_2).collect::<Vec<_>>())),
                Arc::new(Int32Array::from(records.iter().map(|r| r.mioty_qi_3).collect::<Vec<_>>())),
            ],
        )
            .map_err(|e| Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?
    )
}
#[tokio::main]
async fn main() -> Result<()> {
    let warehouse_path = "file:///tmp/iceberg_warehouse";

    let catalog: MemoryCatalog = MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path.to_string())]),
        )
        .await?;

    let table_ident = TableIdent::from_strs(["default", "sd_test"])?;

    let xml_file = Path::new("example.xml");
    let records = parse_sd_file(xml_file)?;

    if records.is_empty() {
        println!("No records found.");
        return Ok(());
    }

    let batch = build_recordbatch(&records)?;
    println!("Parsed {} records successfully.", batch.num_rows());

    Ok(())
}