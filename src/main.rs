use csv;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::sync::{Arc, RwLock};

#[allow(dead_code)]
#[derive(Debug, Deserialize, PartialEq, Eq)]
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
    #[serde(skip_serializing)]
    transactions_history: HashMap<u32, Transaction>,
}

impl Account {
    fn new(id: u16, initial_transaction: Transaction) -> Self {
        Self {
            client: id,
            pending_transactions: VecDeque::from([initial_transaction]),
            ..Self::default()
        }
    }

    fn assert_balance(&self) {
        assert_eq!(self.total, self.available + self.held);
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
            self.assert_balance();
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
                self.assert_balance();
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

    fn dispute(&mut self, transaction_id: u32) -> Result<(), String> {
        match self.transactions_history.get_mut(&transaction_id) {
            Some(transaction) => {
                if transaction.transaction_type == TransactionType::Deposit {
                    let amount = transaction
                        .amount
                        .expect("Transaction stored in transaction_history is valid");

                    transaction.transaction_type = TransactionType::Dispute;
                    self.available -= amount;
                    self.held += amount;
                    self.assert_balance();
                    Ok(())
                } else {
                    Err("Dirpute transaction target was different than Deposit!".into())
                }
            }
            None => Err("Dispute transaction target not valid".into()),
        }
    }

    fn find_dispute_transaction(&mut self, dispute_id: u32) -> Result<&mut Transaction, String> {
        match self.transactions_history.get_mut(&dispute_id) {
            Some(transaction) => {
                if transaction.transaction_type != TransactionType::Dispute {
                    Err("Transaction is not a Dispute transaction".into())
                } else {
                    Ok(transaction)
                }
            }
            None => Err(format!(
                "Transaction with id: {} is not stored in transaction history",
                dispute_id
            )),
        }
    }

    fn resolve(&mut self, dispute_id: u32) -> Result<(), String> {
        let dispute_transaction = self.find_dispute_transaction(dispute_id)?;
        let amount = dispute_transaction
            .amount
            .expect("Dispute transaction stored in history contains amount");

        dispute_transaction.transaction_type = TransactionType::Deposit;
        self.held -= amount;
        self.available += amount;
        self.assert_balance();
        Ok(())
    }

    fn chargeback(&mut self, dispute_id: u32) -> Result<(), String> {
        let dispute_transaction = self.find_dispute_transaction(dispute_id)?;
        let amount = dispute_transaction
            .amount
            .expect("Dispute transaction stored in history contains amount");

        dispute_transaction.transaction_type = TransactionType::Chargeback;
        self.held -= amount;
        self.total -= amount;
        self.locked = true;
        self.assert_balance();
        Ok(())
    }

    fn process_pending_transaction(&mut self) -> Result<(), String> {
        self.is_account_state_valid_for_transaction()?;
        let transaction = match self.pending_transactions.pop_front() {
            Some(t) => t,
            None => return Err("Pending queue is empty, cannot process transaction".into()),
        };
        match transaction.transaction_type {
            TransactionType::Deposit => {
                let amount = match transaction.amount {
                    Some(a) => a,
                    None => {
                        return Err("Deposit is possible only with amount field present".into());
                    }
                };

                self.deposit(amount)?;
                self.transactions_history
                    .insert(transaction.tx, transaction);
            }
            TransactionType::Withdrawal => {
                let amount = match transaction.amount {
                    Some(a) => a,
                    None => {
                        return Err("Withraw is possible only with amount field present".into());
                    }
                };

                self.withdraw(amount)?;
                self.transactions_history
                    .insert(transaction.tx, transaction);
            }
            TransactionType::Dispute => {
                self.dispute(transaction.tx)?;
            }
            TransactionType::Resolve => {
                self.resolve(transaction.tx)?;
            }
            TransactionType::Chargeback => {
                self.chargeback(transaction.tx)?;
            }
        }
        Ok(())
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
        while !account_lock.process_pending_transaction().is_err() {}
        println!("Account state: {:?}", account_lock);
        println!("");
    }

    Ok(())
}
