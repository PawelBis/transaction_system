use csv;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let filename = std::env::args().nth(1).unwrap();

    let mut bank = HashMap::<u16, Arc<RwLock<Account>>>::default();

    let transactions = deserialize_csv_file(filename)?;
    for transaction in transactions {
        match bank.get(&transaction.client) {
            Some(client) => {
                client.write().await.add_transaction(transaction);
            }
            None => {
                bank.insert(
                    transaction.client,
                    Arc::new(RwLock::new(Account::new(transaction.client, transaction))),
                );
            }
        };
    }

    let mut handles = vec!();
    let start = std::time::Instant::now();
    for (_, acc) in bank.iter_mut() {
        let mut account_lock = acc.clone().write_owned().await;

        handles.push(tokio::spawn(async move {
            let mut finish = false;
            while !finish {
                match account_lock.process_pending_transaction() {
                    Ok(_) => {}
                    Err(e) => match e {
                        TransactionProcessingError::NoTransactionToProcess => finish = true,
                        TransactionProcessingError::AccountLocked(_) => {
                            finish = true;
                        }
                        _ => {}
                    },
                };
            }
        }));
    }

    for handle in handles.into_iter() {
        handle.await.unwrap();
    }
    let end = std::time::Instant::now();
    println!(
        "Time: {}",
        end.checked_duration_since(start).unwrap().as_millis()
    );
    Ok(())
}
