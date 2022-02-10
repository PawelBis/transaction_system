use csv;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, RwLock};

use account::Account;
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
    let mut num_transactions = 0;
    let mut num_clients = 0;
    for transaction in transactions {
        num_transactions += 1;
        match bank.get(&transaction.client) {
            Some(client) => {
                client.write().unwrap().add_transaction(transaction);
            }
            None => {
                bank.insert(
                    transaction.client,
                    Arc::new(RwLock::new(Account::new(transaction.client, transaction))),
                );
                num_clients += 1;
            }
        };
    }

    let mut num_acc = 0;
    let mut succ = 0;
    let mut failed = 0;
    let mut locked = 0;
    for (_, acc) in bank.iter_mut() {
        num_acc += 1;
        let mut account_lock = acc.write().unwrap();
        let mut finish = false;
        while !finish {
            match account_lock.process_pending_transaction() {
                Ok(_) => {
                    succ += 1;
                    println!("Processing trancaction on acc {}", num_acc);
                }
                Err(e) => {
                    failed += 1;
                    match e.as_str() {
                        "Pending queue is empty, cannot process transaction" => finish = true,
                        "Account locked!" => {
                            finish = true;
                            locked += account_lock.pending_transactions.len();
                            println!(
                                "Account {} locked with {} transactions left!",
                                num_acc,
                                account_lock.pending_transactions.len()
                            );
                        }
                        _ => {
                            println!("Other error on acc {}: {}", num_acc, e);
                        }
                    }
                }
            };
        }
    }
    println!(
        "Total transactions processed: {}, locked: {}, {}/{}",
        succ + failed,
        locked,
        succ,
        failed
    );

    Ok(())
}
