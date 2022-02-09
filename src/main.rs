use csv;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::sync::{Arc, RwLock};

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

#[derive(Default, Debug, Serialize)]
struct Account {
    client: u16,
    available: f32,
    held: f32,
    total: f32,
    locked: bool,
    #[serde(skip_serializing)]
    pending_transactions: VecDeque<Transaction>,
}

impl Account {
    fn new(id: u16, initial_transaction: Transaction) -> Self {
        Self {
            client: id,
            pending_transactions: VecDeque::from([initial_transaction]),
            ..Self::default()
        }
    }

    fn is_account_state_valid_for_transaction(&self) -> Result<(), String> {
        if self.locked {
            Err("Account locked!".into())
        } else {
            Ok(())
        }
    }

    fn deposit(&mut self, amount: f32) -> Result<(), String> {
        self.is_account_state_valid_for_transaction()?;

        if amount > 0.0 {
            self.available += amount;
            self.total += amount;
            assert_eq!(self.total, self.available + self.held);
            Ok(())
        } else {
            Err(format!("deposit amount: {} is not valid", amount).into())
        }
    }

    fn withdraw(&mut self, amount: f32) -> Result<(), String> {
        self.is_account_state_valid_for_transaction()?;

        if amount > 0.0 {
            if self.available - amount >= 0.0 {
                self.total -= amount;
                self.available -= amount;
                assert_eq!(self.total, self.available + self.held);
                Ok(())
            } else {
                Err(format!(
                    "Account available resources: {} are lower than withdraw amount: {}",
                    self.available, amount
                )
                .to_string())
            }
        } else {
            Err(format!("withdraw called with amount {} which is not valid", amount).to_string())
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

    let mut transaction_to_client_map = HashMap::<u32, u16>::default();
    let mut bank = HashMap::<u16, Arc<RwLock<Account>>>::default();

    let transactions = deserialize_csv_file(filename)?;
    for transaction in transactions {
        transaction_to_client_map.insert(transaction.tx, transaction.client);

        match bank.get(&transaction.client) {
            Some(client) => {
                client
                    .write()
                    .unwrap()
                    .pending_transactions
                    .push_back(transaction);
            }
            None => {
                bank.insert(
                    transaction.client,
                    Arc::new(RwLock::new(Account::new(transaction.client, transaction))),
                );
            }
        };
    }

    for (_, acc) in bank.iter_mut() {
        println!("Analyzing account transaciton");
        let mut account_lock = acc.write().unwrap();
        while let Some(transaction) = account_lock.pending_transactions.pop_front() {
            println!("Analyzing transaciton");
            match transaction.transaction_type {
                TransactionType::Deposit => {
                    let amount = match transaction.amount {
                        Some(a) => a,
                        None => {
                            println!("Deposit is possible only with amount field present");
                            continue;
                        }
                    };

                    if let Err(e) = account_lock.deposit(amount) {
                        println!("Deposit transaction failed: {}", e);
                    }
                }
                TransactionType::Withdrawal => {
                    let amount = match transaction.amount {
                        Some(a) => a,
                        None => {
                            println!("Withraw is possible only with amount field present");
                            continue;
                        }
                    };

                    if let Err(e) = account_lock.withdraw(amount) {
                        println!("Deposit transaction failed: {}", e);
                    }
                }
                TransactionType::Dispute => (),
                TransactionType::Resolve => (),
                TransactionType::Chargeback => (),
            }
            println!("Finished transaciton");
        }
        println!("Account state: {:?}", account_lock);
        println!("");
    }

    Ok(())
}
