# OptME
OptME is proposed in the paper "***Toward High-Performance Blockchain System by Blurring the Line between Ordering and Execution***" at ACM/IEEE Supercomputing Conference 2024 (SC '24). 
OptME is a novel *deterministic orderable concurrency control algorithm* for high performance blockchain systems, which efficiently utilizes latent parallelism among the transactions given by the consensus phase.
OptME is blazingly fast, e.g., up to 350k TPS with a machine equipped with Intel i9-13900 CPU, 64GB DDR5 RAM and 2TB SSD. 

# How to benchmark?
Note that we only use the codes in the path `crates/sslab-execution`. 
All the other files are not related with OptME and will be not even compiled.

#### 1. Install dependencies
```bash
sudo apt-get update
sudo apt-get -y upgrade
sudo apt-get -y autoremove

# The following dependencies prevent the linking error.
sudo apt-get -y install build-essential
sudo apt-get -y install cmake

# Install rust (non-interactive).
sudo apt-get -y install curl
curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
rustup update
rustup default stable

# This is missing from the Rocksdb installer (needed for Rocksdb).
sudo apt-get install -y clang
sudo apt-get install pkg-config
sudo apt-get install libssl-dev

# Install protobuf.
sudo apt-get install -y protobuf-compiler

# Clone the repo.
sudo apt-get install -y git
git clone https://github.com/Dong-Hyeon-Yu/optme.git
```
#### 2. Run benchmarks
The workload for benchmarks is Smallbank workload.

```bash
cd crates/sslab-execution/optme/benches
cargo bench -- blocksize > baseline.log

```
Please see `Appendix: Artifact Description/Artifact Evaluation` of the paper if you want to reproduce the figures in the paper,

#### 3. Parsing the results
If you want to process the result in Excel or elsewhere, use the parsing scripts located in each `benches` folder as following:
```bash
# required python version >= 3.10
python3 parse_log.py optme-tps.log  # output filename is 'optme.log.out'
```
