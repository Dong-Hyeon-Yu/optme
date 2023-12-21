# Copyright(C) Facebook, Inc. and its affiliates.
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime, timezone
from itertools import chain
from dateutil import parser
from glob import glob
from logging import exception
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean


from benchmark.utils import ExecutionModel, Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, primaries, workers, execution_model, faults=0, concurrency_level=0):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.execution_model = execution_model
        
        self.concurrency_level = concurrency_level if execution_model == ExecutionModel.NEZHA else 1
        
        

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers = len(workers) // len(primaries)
        else:
            self.committee_size = 4
            self.workers = 1

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            exception(e)
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        self.rate, self.start, misses, self.sent_samples, skewness \
            = zip(*results)
        self.misses = sum(misses)
        self.skewness = skewness[0]

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_consensus, primaries)
                execution_results = p.map(self._parse_executions, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            exception(e)
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        proposals, orders, self.configs, primary_ips, batch_to_header_latencies, header_creation_latencies, header_to_cert_latencies, cert_commit_latencies, request_vote_outbound_latencies = zip(
            *results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.orders = self._merge_results([x.items() for x in orders])

        self.batch_to_header_latencies = {
            k: v for x in batch_to_header_latencies for k, v in x.items()
        }
        self.header_creation_latencies = {
            k: v for x in header_creation_latencies for k, v in x.items()
        }
        self.header_to_cert_latencies = {
            k: v for x in header_to_cert_latencies for k, v in x.items()
        }
        self.cert_commit_latencies = {
            k: v for x in cert_commit_latencies for k, v in x.items()
        }
        self.request_vote_outbound_latencies = list(
            chain(*request_vote_outbound_latencies))
        
        # execution metrics
        commits, subdag_size, aborted, total, subscriber_receive, handler_receive, execution_receive = zip(*execution_results)
        self.commits = self._merge_results([x.items() for x in commits])
        self.subdag_size = self._merge_results([x.items() for x in subdag_size])
        self.subscriber_receive = self._merge_results([x.items() for x in subscriber_receive])
        self.handler_receive = self._merge_results([x.items() for x in handler_receive])
        self.execution_receive = self._merge_results([x.items() for x in execution_receive])
        self.aborted = sum([int(x) for single_list in aborted for x in single_list]) / self.committee_size
        self.total = sum([int(x) for single_list in total for x in single_list]) / self.committee_size

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            exception(e)
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, workers_ips, batch_creation_latencies, tx_num = zip(
            *results)

        self.tx_num = {digest: _tx_num for x in tx_num for digest, _tx_num in x.items()}
        self.total_sending_tx = sum(len(x) for x in self.sent_samples)
        self.total_received_tx = sum(len(x) for x in self.received_samples)
        self.total_ordered_tx = sum(self.tx_num[digest] for digest, _ in self.orders.items() if digest in self.tx_num)
        self.total_committed_tx = sum(self.tx_num[digest] for digest, _ in self.commits.items() if digest in self.tx_num)

        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.orders
        }

        self.commit_sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        self.batch_creation_latencies = {
            k: v for x in batch_creation_latencies for k, v in x.items()
        }

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if k not in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'(.*?) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'(.*?) .* sample transaction (\d+)', log)
        samples = {int(tx_id): self._to_posix(t) for t, tx_id in tmp}
        
        skewness = float(search(r'Workload skewness: (\d+\.\d+)', log).group(1))

        return rate, start, misses, samples, skewness
    
    def _parse_executions(self, log):
        if search(r'(?:panicked)', log) is not None:
            raise ParseError('Primary(s) panicked')
        
        tmp = findall(r'(.*?) .* Subscriber received a batch -> ([^ ]+=)', log)
        tmp = [(digest, self._to_posix(t)) for t, digest in tmp]
        subsriber_receive = self._merge_results([tmp])
        
        tmp = findall(r'(.*?) .* Consensus handler received a batch -> ([^ ]+=)', log)
        tmp = [(digest, self._to_posix(t)) for t, digest in tmp]
        handler_receive = self._merge_results([tmp])
        
        tmp = findall(r'.*? .* Received consensus_output has (\d+) batches at subdag_index (\d+).', log)
        subdag_size = [(i, int(s)) for s, i in tmp]
        subdag_size = self._merge_results([subdag_size])

        tmp = findall(r'(.*?) .* Received Batch -> ([^ ]+=)', log)
        tmp = [(digest, self._to_posix(t)) for t, digest in tmp]
        execution_receive = self._merge_results([tmp])

        tmp = findall(r'(.*?) .* Executed Batch -> ([^ ]+=)', log)
        tmp = [(digest, self._to_posix(t)) for t, digest in tmp]
        commits = self._merge_results([tmp])
        
        tmp = findall(r'Abort rate: \d+.\d+ \((\d+)/(\d+) aborted\)', log)
        
        aborted, total = zip(*tmp) if tmp else ([0], [0])
        

        return commits, subdag_size, aborted, total, subsriber_receive, handler_receive, execution_receive

    def _parse_consensus(self, log):
        if search(r'(?:panicked)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'(.*?) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'(.*?) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        orders = self._merge_results([tmp])

        tmp = findall(
            r'.* Batch ([^ ]+) from worker \d+ took (\d+\.\d+) seconds from creation to be included in a proposed header', log)
        batch_to_header_latencies = {d: float(t) for d, t in tmp}

        tmp = findall(
            r'.* Header ([^ ]+) was created in (\d+\.\d+) seconds', log)
        header_creation_latencies = {d: float(t) for d, t in tmp}

        tmp = findall(
            r'.* Header ([^ ]+) at round \d+ with \d+ batches, took (\d+\.\d+) seconds to be materialized to a certificate [^ ]+', log)
        header_to_cert_latencies = {d: float(t) for d, t in tmp}

        tmp = findall(
            r'.* Certificate ([^ ]+) took (\d+\.\d+) seconds to be committed at round \d+', log)
        cert_commit_latencies = {d: float(t) for d, t in tmp}

        tmp = findall(
            r'\/narwhal\.PrimaryToPrimary\/RequestVote.*direction=outbound.*latency=(\d+) ms', log)
        request_vote_outbound_latencies = [float(d) for d in tmp]

        configs = {
            'header_num_of_batches_threshold': int(
                search(r'Header number of batches threshold .* (\d+)', log).group(1)
            ),
            'max_header_num_of_batches': int(
                search(r'Header max number of batches .* (\d+)', log).group(1)
            ),
            'max_header_delay': int(
                search(r'Max header delay .* (\d+)', log).group(1)
            ),
            'gc_depth': int(
                search(r'Garbage collection depth .* (\d+)', log).group(1)
            ),
            'sync_retry_delay': int(
                search(r'Sync retry delay .* (\d+)', log).group(1)
            ),
            'sync_retry_nodes': int(
                search(r'Sync retry nodes .* (\d+)', log).group(1)
            ),
            'batch_size': int(
                search(r'Batch size .* (\d+)', log).group(1)
            ),
            'max_batch_delay': int(
                search(r'Max batch delay .* (\d+)', log).group(1)
            ),
            'max_concurrent_requests': int(
                search(r'Max concurrent requests .* (\d+)', log).group(1)
            )
        }

        ip = search(r'booted on (/ip4/\d+.\d+.\d+.\d+)', log).group(1)

        return proposals, orders, configs, ip, batch_to_header_latencies, header_creation_latencies, header_to_cert_latencies, cert_commit_latencies, request_vote_outbound_latencies

    def _parse_workers(self, log):
        if search(r'(?:panicked)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B with (\d+)', log)
        sizes = {digest: int(bytes) for digest, bytes, _ in tmp}
        tx_num = {digest: int(tx_num) for digest, _, tx_num in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(tx_id): digest for digest, tx_id in tmp}

        tmp = findall(
            r'.* Batch ([^ ]+) took (\d+\.\d+) seconds to create due to .*', log)
        batch_creation_latencies = {digest: float(t) for digest, t in tmp}

        ip = search(r'booted on (/ip4/\d+.\d+.\d+.\d+)', log).group(1)

        return sizes, samples, ip, batch_creation_latencies, tx_num

    def _to_posix(self, string):
        x = parser.parse(string[:24], ignoretz=True)
        x = x.astimezone(timezone.utc)
        return datetime.timestamp(x)

    def _consensus_throughput(self):
        if not self.orders:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.orders.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = self.total_ordered_tx / duration
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.orders.items()]
        return mean(latency) if latency else 0
    
    def _execution_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.orders.values()), max(self.commits.values())
        duration = end - start
        bytes = sum(self.commit_sizes.values())
        bps = bytes / duration
        tps = self.total_committed_tx / duration
        return tps, bps, duration
    
    def _consensus_to_execution_latency(self):
        latency = [c - self.orders[d] for d, c in self.subscriber_receive.items()]
        return mean(latency) if latency else 0
    
    def _subscriber_latency(self):
        latency = [c - self.subscriber_receive[d] for d, c in self.handler_receive.items()]
        return mean(latency) if latency else 0
    
    def _consensus_handler_latency(self):
        latency = [c - self.handler_receive[d] for d, c in self.execution_receive.items()]
        return mean(latency) if latency else 0
    
    def _batch_execution_latency(self):
        latency = [c - self.execution_receive[d] for d, c in self.commits.items()]
        return mean(latency) if latency else 0
    
    def _execution_latency(self):
        latency = [c - self.orders[d] for d, c in self.commits.items()]
        return mean(latency) if latency else 0


    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.commits.values())
        duration = end - start
        bytes = sum(self.commit_sizes.values())
        bps = bytes / duration
        tps = self.total_committed_tx / duration
        return tps, bps, duration

    def _end_to_end_latency(self):
        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.commits:
                    assert tx_id in sent.keys()  # We receive txs that we sent.
                    start = sent[tx_id]
                    end = self.commits[batch_id]
                    latency += [end-start]
        return mean(latency) if latency else 0

    def result(self):
        header_num_of_batches_threshold = self.configs[0]['header_num_of_batches_threshold']
        max_header_num_of_batches = self.configs[0]['max_header_num_of_batches']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']
        max_concurrent_requests = self.configs[0]['max_concurrent_requests']

        consensus_latency = self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, duration = self._consensus_throughput()

        sending_latency_to_execution = self._consensus_to_execution_latency() * 1_000
        subscriber_latency = self._subscriber_latency() * 1_000
        handler_latency = self._consensus_handler_latency() * 1_000
        batch_execution_latency = self._batch_execution_latency() * 1_000
        execution_latency = self._execution_latency() * 1_000
        execution_tps, execution_bps, excution_duration = self._execution_throughput()
        
        
        abort_rate = self.aborted / self.total if self.aborted else 0.0
        effective_tps = execution_tps * (1-abort_rate) if self.aborted else execution_tps
        abort_rate *= 100
        
        end_to_end_tps, end_to_end_bps, _ = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1_000

        # TODO: support primary and worker on different processes, and fail on
        # empty log entries.
        batch_creation_latency = mean(
            self.batch_creation_latencies.values()) * 1000 if self.batch_creation_latencies else -1
        header_creation_latency = mean(
            self.header_creation_latencies.values()) * 1000 if self.header_creation_latencies else -1
        batch_to_header_latency = mean(
            self.batch_to_header_latencies.values()) * 1000 if self.batch_to_header_latencies else -1
        header_to_cert_latency = mean(
            self.header_to_cert_latencies.values()) * 1000 if self.header_to_cert_latencies else -1
        cert_commit_latency = mean(
            self.cert_commit_latencies.values()) * 1000 if self.cert_commit_latencies else -1
        request_vote_outbound_latency = mean(
            self.request_vote_outbound_latencies) if self.request_vote_outbound_latencies else -1

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Input skewness: {self.skewness:.1f} \n'
            # f' Transaction size (avg.): {(sum(self.sizes.values()) / len(self.sent_samples)):,} B\n'
            f' Execution time: {round(duration):,} s\n'
            f' Execution mode: {self.execution_model} \n'
            f' Concurrency level: {self.concurrency_level} \n'
            '\n'
            f' Header number of batches threshold: {header_num_of_batches_threshold:,} digests\n'
            f' Header maximum number of batches: {max_header_num_of_batches:,} digests\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            f' Max concurrent requests: {max_concurrent_requests:,} \n'
            '\n'
            ' + RESULTS:\n'
            f' Batch creation avg latency: {round(batch_creation_latency):,} ms\n'
            f' Header creation avg latency: {round(header_creation_latency):,} ms\n'
            f' \tBatch to header avg latency: {round(batch_to_header_latency):,} ms\n'
            f' Header to certificate avg latency: {round(header_to_cert_latency):,} ms\n'
            f' \tRequest vote outbound avg latency: {round(request_vote_outbound_latency):,} ms\n'
            f' Average Batch size: {round(mean(self.sizes.values())/1024)} KB\n'
            f' Average Subdag size: {round(mean(self.subdag_size.values()))} \n'
            f' \tMax Subdag size: {max(self.subdag_size.values())} \n'
            f' \tMin Subdag size: {min(self.subdag_size.values())} \n'
            f' Average Transaction size: {round(sum(self.sizes.values()) / self.total_ordered_tx)} B\n'
            f' \tActual Sending Rate: {round(self.total_sending_tx / duration):,} tx/s\n'
            f' \tTotal Sending Transactions: {self.total_sending_tx} tx\n'
            f' \tTotal Received Transactions: {self.total_received_tx} tx\n'
            f' \tTotal Ordered Transactions: {self.total_ordered_tx} tx\n'
            f' \tTotal Committed Transactions: {self.total_committed_tx} tx\n'
            f' Certificate commit avg latency: {round(cert_commit_latency):,} ms\n'
            f'\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            f'\n'
            f' Execution TPS: {round(execution_tps):,} tx/s\n'
            f' Execution BPS: {round(execution_bps):,} B/s\n'
            f' Execution latency: {round(execution_latency):,} ms\n'
            f' \tConsensus to execution latency: {round(sending_latency_to_execution):,} ms\n'
            f' \tSubscriber latency: {round(subscriber_latency):,} ms\n'
            f' \tConsensus handler latency: {round(handler_latency):,} ms\n'
            f' \tBatch execution latency: {round(batch_execution_latency):,} ms\n'
            f' \tAverage Abort Rate: {abort_rate:.2f} % \n'
            f' \tEffective TPS: {round(effective_tps):,} tx/s\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, execution_model, faults=0, concurrency_level=1):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]
        workers = []
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            with open(filename, 'r') as f:
                workers += [f.read()]

        return cls(clients, primaries, workers, execution_model, faults=faults, concurrency_level=concurrency_level)


class LogGrpcParser:
    def __init__(self, primaries, faults=0):
        assert all(isinstance(x, str) for x in primaries)
        self.faults = faults

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            exception(e)
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        self.grpc_ports = results

    def _parse_primaries(self, log):
        port = search(
            r'Consensus API gRPC Server listening on /ip4/.+/tcp/(.+)/http', log).group(1)
        return port

    @classmethod
    def process(cls, directory, faults=0):
        assert isinstance(directory, str)

        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]

        return cls(primaries, faults=faults)
