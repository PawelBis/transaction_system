use csv;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, RwLock};

use account::{Account, TransactionProcessingError};
mod account;

#[allow(dead_code)]
#[derive(Debug, Deserialize, PartialEq, Eq)]
pub enum TransactionType {
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
pub struct Transaction {
    #[serde(rename = "type")]
    transaction_type: TransactionType,
    client: u16,
    tx: u32,
    amount: Option<f32>,
}

impl Transaction {
    pub fn new(
        transaction_type: TransactionType,
        client: u16,
        tx: u32,
        amount: Option<f32>,
    ) -> Self {
        Self {
            transaction_type,
            client,
            tx,
            amount,
        }
    }
}

fn deserialize_csv_file(path: String) -> Result<Vec<Transaction>, Box<dyn Error>> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)
        .unwrap();

    let mut transactions = Vec::<Transaction>::default();

    for transaction in reader.deserialize() {
        transactions.push(transaction?);
    }

    Ok(transactions)
}

fn main() -> Result<(), Box<dyn Error>> {
    let filename = std::env::args().nth(1).unwrap();

    let mut bank = HashMap::<u16, Arc<RwLock<Account>>>::default();

    let transactions = deserialize_csv_file(filename)?;
    for transaction in transactions {
        match bank.get(&transaction.client) {
            Some(client) => {
                client.write().unwrap().add_transaction(transaction);
            }
            None => {
                bank.insert(
                    transaction.client,
                    Arc::new(RwLock::new(Account::new(transaction.client, transaction))),
                );
            }
        };
    }

    let start = std::time::Instant::now();
    let mut successful_transactions = 0;
    let mut failed_transactions = 0;
    let mut locked_transactions = 0;
    for (_, acc) in bank.iter_mut() {
        let mut account_lock = acc.write().unwrap();
        let mut finish = false;
        while !finish {
            match account_lock.process_pending_transaction() {
                Ok(_) => {
                    successful_transactions += 1;
                }
                Err(e) => match e {
                    TransactionProcessingError::NoTransactionToProcess => finish = true,
                    TransactionProcessingError::AccountLocked(number_of_locked_transactions) => {
                        finish = true;
                        locked_transactions += number_of_locked_transactions;
                    }
                    _ => {
                        failed_transactions += 1;
                    }
                },
            };
        }
    }

    let end = std::time::Instant::now();

    println!(
        "Total transactions processed: {}, locked: {}, {}/{}",
        successful_transactions + failed_transactions,
        locked_transactions,
        successful_transactions,
        failed_transactions
    );
    println!("Time: {}", end.checked_duration_since(start).unwrap().as_millis());

    Ok(())
}
