use account::Account;
use csv;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

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

fn deserialize_csv_file(path: String, sender: mpsc::UnboundedSender<Transaction>) {
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
    let filename = match std::env::args().nth(1) {
        Some(f) => f,
        None => {
            return Err("Please provide csv filename".into());
        }
    };

    let mut bank = HashMap::<u16, Arc<Mutex<Account>>>::default();

    let (tx, mut px) = mpsc::unbounded_channel::<Transaction>();
    tokio::task::spawn_blocking(move || {
        deserialize_csv_file(filename.to_string(), tx);
    });

    while let Some(transaction) = px.recv().await {
        let client = match bank.get(&transaction.client) {
            Some(client) => client.clone(),
            None => {
                let new_client = Arc::new(Mutex::new(Account::new(transaction.client)));
                bank.insert(transaction.client, new_client.clone());

                new_client
            }
        };

        tokio::spawn(async move {
            let mut client = client.lock_owned().await;
            client.add_transaction(transaction);
            client.process_pending_transaction()
        });
    }

    let mut writer = csv::Writer::from_writer(std::io::stdout());
    for (_, account) in bank {
        writer.serialize(account.lock().await.to_owned())?;
    }

    Ok(())
}
