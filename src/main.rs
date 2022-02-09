use csv;
use csv::ErrorKind;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
enum TransactionType {
    #[serde(rename = "deposit")]
    Deposit,
    #[serde(rename = "withdrawal")]
    Withdrawal,
    #[serde(rename = "dispute")]
    Dispute,
    #[serde(rename = "resolve")]
    Resolve,
    #[serde(rename = "chargeback")]
    Chargeback,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct Transaction {
    #[serde(rename = "type")]
    transaction_type: TransactionType,
    client: u16,
    tx: u32,
    amount: Option<f32>,
}

#[derive(Serialize)]
struct Account {
    client: u16,
    available: f32,
    held: f32,
    total: f32,
    locked: bool,
}

fn example(path: String) -> Result<(), Box<dyn Error>> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)
        .unwrap();

    let mut transactions = HashMap::<u32, Transaction>::default();
    let mut accounts = HashMap::<u16, Account>::default();

    println!("Deserializing");
    for result in reader.deserialize() {
        let record: Transaction = result?;
        println!("{:?}", record);
    }
    Ok(())
}

fn main() {
    let filename = std::env::args().nth(1).unwrap();

    if let Err(err) = example(filename) {
        println!("Error running example: {}", err);
    }
}
