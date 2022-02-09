use super::{Transaction, TransactionType};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};

#[derive(Default, Debug, Serialize)]
pub struct Account {
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
    pub fn new(id: u16, initial_transaction: Transaction) -> Self {
        Self {
            client: id,
            pending_transactions: VecDeque::from([initial_transaction]),
            ..Self::default()
        }
    }

    pub fn add_transaction(&mut self, new_transaction: Transaction) {
        self.pending_transactions.push_back(new_transaction);
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

    pub fn process_pending_transaction(&mut self) -> Result<(), String> {
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

#[cfg(test)]
mod tests {
    use super::{Account, Transaction, TransactionType};

    fn prepare_acc(initial_funds: f32) -> Account {
        let mut acc = Account::new(
            0,
            Transaction::new(TransactionType::Deposit, 0, 0, Some(initial_funds)),
        );
        acc.process_pending_transaction().unwrap();
        acc
    }

    #[test]
    fn deposit() {
        let mut acc = prepare_acc(5.0);
        assert_eq!(acc.available, 5.0);
        assert_eq!(acc.total, 5.0);

        acc.add_transaction(Transaction::new(TransactionType::Deposit, 0, 1, Some(-5.0)));
        assert!(acc.process_pending_transaction().is_err());
        assert_eq!(acc.available, 5.0);
        assert_eq!(acc.total, 5.0);
    }

    #[test]
    fn withdraw() {
        let mut acc = prepare_acc(10.0);
        assert_eq!(acc.available, 10.0);
        assert_eq!(acc.total, 10.0);

        acc.add_transaction(Transaction::new(
            TransactionType::Withdrawal,
            0,
            1,
            Some(5.0),
        ));
        acc.process_pending_transaction().unwrap();
        assert_eq!(acc.available, 5.0);
        assert_eq!(acc.total, 5.0);

        acc.add_transaction(Transaction::new(
            TransactionType::Withdrawal,
            0,
            2,
            Some(6.0),
        ));
        assert!(acc.process_pending_transaction().is_err());
        assert_eq!(acc.available, 5.0);
        assert_eq!(acc.total, 5.0);

        acc.add_transaction(Transaction::new(
            TransactionType::Withdrawal,
            0,
            3,
            Some(-1.0),
        ));
        assert!(acc.process_pending_transaction().is_err());
        assert_eq!(acc.available, 5.0);
        assert_eq!(acc.total, 5.0);
    }

    #[test]
    fn dispute() {
        let mut acc = prepare_acc(10.0);
        assert_eq!(acc.available, 10.0);
        assert_eq!(acc.total, 10.0);
        const TRANSACTION_TO_DISPUTE_ID: u32 = 5;

        let deposit_transaction = Transaction::new(
            TransactionType::Deposit,
            0,
            TRANSACTION_TO_DISPUTE_ID,
            Some(5.0),
        );
        acc.add_transaction(deposit_transaction);
        acc.process_pending_transaction().unwrap();

        let dispute_transaction =
            Transaction::new(TransactionType::Dispute, 0, TRANSACTION_TO_DISPUTE_ID, None);

        acc.add_transaction(dispute_transaction);
        acc.process_pending_transaction().unwrap();
        assert_eq!(acc.total, 15.0);
        assert_eq!(acc.available, 10.0);
        assert_eq!(acc.held, 5.0);
    }
}
