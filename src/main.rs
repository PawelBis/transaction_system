use csv;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::mpsc;
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

fn async_deserialize_csv_file(path: String, sender: mpsc::UnboundedSender<Transaction>) {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)
        .unwrap();

    for transaction in reader.deserialize() {
        if let Ok(t) = transaction {
            let _ = sender.send(t);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let filename = std::env::args().nth(1).unwrap();
    let start = std::time::Instant::now();

    let mut bank = HashMap::<u16, Arc<RwLock<Account>>>::default();
    let (tx, mut px) = mpsc::unbounded_channel::<Transaction>();

    tokio::task::spawn_blocking(move || {
        async_deserialize_csv_file(filename, tx);
    });

    let mut handles = vec![];
    while let Some(t) = px.recv().await {
        let c = match bank.get(&t.client) {
            Some(client) => {
                client.clone()
                //client.write().await.
            }
            None => {
                let new_client = Arc::new(RwLock::new(Account::new(t.client)));
                let _ = bank.insert(t.client, new_client.clone());

                new_client
            }
        };

        handles.push(tokio::spawn(async move {
            let mut c = c.write_owned().await;
            c.add_transaction(t);
            let _ = c.process_pending_transaction();
        }));
    }

    //for transaction in transactions {
    //    match bank.get(&transaction.client) {
    //        Some(client) => {
    //            client.write().await.add_transaction(transaction);
    //        }
    //        None => {
    //            bank.insert(
    //                transaction.client,
    //                Arc::new(RwLock::new(Account::new(transaction.client, transaction))),
    //            );
    //        }
    //    };
    //}

    //for (_, acc) in bank.iter_mut() {
    //    let mut account_lock = acc.clone().write_owned().await;

    //    handles.push(tokio::spawn(async move {
    //        let mut finish = false;
    //        while !finish {
    //            match account_lock.process_pending_transaction() {
    //                Ok(_) => {}
    //                Err(e) => match e {
    //                    TransactionProcessingError::NoTransactionToProcess => finish = true,
    //                    TransactionProcessingError::AccountLocked(_) => {
    //                        finish = true;
    //                    }
    //                    _ => {}
    //                },
    //            };
    //        }
    //    }));
    //}

    let joining = std::time::Instant::now();
    for handle in handles.into_iter() {
        handle.await.unwrap();
    }
    let joininge = std::time::Instant::now();
    println!(
        "Joining Time: {}",
        joininge
            .checked_duration_since(joining)
            .unwrap()
            .as_millis()
    );

    let end = std::time::Instant::now();
    println!(
        "Time: {}",
        end.checked_duration_since(start).unwrap().as_millis()
    );
    Ok(())
}
