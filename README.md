## Immutable Ledger used in Plenum. 

This codebase provides a simple, python-based, immutable, ordered log of transactions 
backed by a merkle tree. This is an efficient way to generate verifiable proofs of presence
and data consistency.

The scope of concerns here is fairly narrow; it is not a full-blown
distributed ledger technology like Fabric, but simply the persistence
mechanism that Plenum needs. The repo is intended to be collapsed into the indy-node codebase
over time; hence there is no wiki, no documentation, and no intention to
use github issues to track bugs.

You can log issues against this codebase in [Hyperledger's Jira](https://jira.hyperledger.org).

Join us on [Hyperledger's Rocket.Chat](http://chat.hyperledger.org), on the #indy
channel, to discuss.

