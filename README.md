# transaction-system

I decided to use mpsc, even though it is slower than traditional multi threaded processing, because it allows for easy repurposing to receiving transactions from multiple ends.

# DSafety problems
- Right now there is a risk of f32 overflow during account serialization. I should probably use f64 in an Account struct.
- Another problem is the fact, that I am storing an account's total value in a field, but perfectly this value should be calculated basing on Account.available and Account.held
