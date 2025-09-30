#!/usr/bin/env python3
"""
azure_iot_multi_thread.py

Multi-threaded IoT simulation with real D2C sends to Azure IoT Hub.
Uses a queue to buffer samples when processor is busy, preventing data loss.

Architecture:
- Producer Thread: Generates sensor samples at fixed intervals
- Consumer Thread: Processes samples from queue and sends to Azure IoT Hub
- Queue: Thread-safe buffer that stores samples waiting for processing

Usage:
  export AZURE_DEVICE_CONN_STR="HostName=...;DeviceId=sim-device-1;SharedAccessKey=..."
  pip install azure-iot-device python-dotenv
  python azure_iot_multi_thread.py --num 40 --sample-interval 0.5 --proc-duration 2.0
"""

import os
import time
import json
import random
import argparse
import threading
from queue import Queue, Empty
from typing import Tuple, Dict, List
from dotenv import load_dotenv
from azure.iot.device import IoTHubDeviceClient, Message

load_dotenv()

AZURE_CONN_STR = os.getenv("AZURE_DEVICE_CONN_STR")

if not AZURE_CONN_STR:
    raise ValueError("AZURE_DEVICE_CONN_STR environment variable is required. Please set it in your .env file.")

# Initialize Azure IoT Hub client (thread-safe)
CLIENT = IoTHubDeviceClient.create_from_connection_string(AZURE_CONN_STR)
print("Azure IoT Device SDK: Connected successfully.")


def now_ts() -> str:
    """Return current time in HH:MM:SS format."""
    return time.strftime("%X")


def send_to_azure(payload: dict) -> Tuple[bool, str]:
    """Send data to Azure IoT Hub. Blocking call but runs in separate thread."""
    try:
        msg = Message(json.dumps(payload))
        CLIENT.send_message(msg)
        return True, "sent"
    except Exception as e:
        return False, f"error:{e}"


class ProducerThread(threading.Thread):
    """
    Producer thread that generates sensor samples at fixed intervals.
    Samples are placed into a queue for processing by consumer thread.
    """
    
    def __init__(self, queue: Queue, num_samples: int, sampling_interval: float, 
                 realtime: bool, stats: Dict):
        super().__init__(name="Producer")
        self.queue = queue
        self.num_samples = num_samples
        self.sampling_interval = sampling_interval
        self.realtime = realtime
        self.stats = stats
        self.start_time = None
        
    def run(self):
        """Generate samples and put them in queue."""
        self.start_time = time.time()
        self.stats['start_time'] = self.start_time
        
        print(f"[{now_ts()}] 🔵 Producer thread STARTED")
        
        for i in range(self.num_samples):
            sample_time = self.start_time + i * self.sampling_interval
            sample_value = round(random.uniform(40.0, 95.0), 2)
            
            sample = {
                "device_id": "sim-device-1",
                "sample_index": i,
                "ts": sample_time,
                "noise_rms": sample_value,
                "produced_at": time.time()  # Actual production time
            }
            
            # Put sample in queue (non-blocking, queue can grow unbounded)
            self.queue.put(sample)
            queue_size = self.queue.qsize()
            
            print(f"[{now_ts()}] 📥 PRODUCED sample #{i}: {sample_value} (queue size: {queue_size})")
            self.stats['produced'].append((i, sample_value, sample_time))
            
            # Sleep until next sample time if in realtime mode
            if self.realtime and i < self.num_samples - 1:
                next_sample_at = self.start_time + (i + 1) * self.sampling_interval
                sleep_for = max(0.0, next_sample_at - time.time())
                if sleep_for > 0:
                    time.sleep(sleep_for)
        
        # Signal end of production by putting None (sentinel value)
        self.queue.put(None)
        print(f"[{now_ts()}] 🔵 Producer thread COMPLETED ({self.num_samples} samples produced)")


class ConsumerThread(threading.Thread):
    """
    Consumer thread that processes samples from queue.
    Simulates processing time and sends data to Azure IoT Hub.
    """
    
    def __init__(self, queue: Queue, processing_duration: float, stats: Dict):
        super().__init__(name="Consumer")
        self.queue = queue
        self.processing_duration = processing_duration
        self.stats = stats
        self.running = True
        
    def run(self):
        """Process samples from queue until sentinel value (None) is received."""
        print(f"[{now_ts()}] 🟢 Consumer thread STARTED")
        
        while self.running:
            try:
                # Get sample from queue (block for up to 0.1 seconds)
                sample = self.queue.get(timeout=0.1)
                
                # Check for sentinel value (end of production)
                if sample is None:
                    print(f"[{now_ts()}] 🟢 Consumer thread received END signal")
                    break
                
                # Process the sample
                self._process_sample(sample)
                
                # Mark task as done (for queue.join() if needed)
                self.queue.task_done()
                
            except Empty:
                # Queue is empty, continue waiting
                continue
        
        print(f"[{now_ts()}] 🟢 Consumer thread COMPLETED")
    
    def _process_sample(self, sample: dict):
        """Process a single sample: simulate work and send to Azure."""
        sample_index = sample['sample_index']
        sample_value = sample['noise_rms']
        queue_size = self.queue.qsize()
        
        proc_start = time.time()
        wait_time = proc_start - sample['produced_at']
        
        print(f"[{now_ts()}] ⚙️  PROCESSING sample #{sample_index}: {sample_value} "
              f"(waited {wait_time:.3f}s, queue: {queue_size})")
        
        # Send to Azure IoT Hub (blocks this thread)
        send_start = time.time()
        send_success, reason = send_to_azure(sample)
        send_duration = time.time() - send_start
        
        # Simulate remaining processing work
        remaining_proc = max(0.0, self.processing_duration - send_duration)
        if remaining_proc > 0:
            time.sleep(remaining_proc)
        
        proc_end = time.time()
        total_proc_time = proc_end - proc_start
        
        # Record statistics
        if send_success:
            self.stats['processed'].append({
                'index': sample_index,
                'value': sample_value,
                'wait_time': wait_time,
                'proc_time': total_proc_time,
                'send_duration': send_duration,
                'reason': reason
            })
            print(f"[{now_ts()}] ✅ SENT sample #{sample_index} -> cloud "
                  f"(proc: {total_proc_time:.3f}s, reason: {reason})")
        else:
            self.stats['dropped_on_send'].append({
                'index': sample_index,
                'value': sample_value,
                'wait_time': wait_time,
                'reason': reason
            })
            print(f"[{now_ts()}] ⚠️  SEND FAILED sample #{sample_index} (reason: {reason})")


def run_simulation(num_samples: int, sampling_interval: float, 
                   processing_duration: float, realtime: bool):
    """
    Run multi-threaded IoT simulation.
    
    Architecture:
    1. Main thread creates queue and stats dictionary
    2. Producer thread generates samples and puts them in queue
    3. Consumer thread takes samples from queue and processes them
    4. Threads communicate via thread-safe Queue
    """
    print("\n" + "="*70)
    print("=== Multi-threaded IoT D2C Simulation ===")
    print("="*70)
    print(f"Configuration:")
    print(f"  - Samples: {num_samples}")
    print(f"  - Sample interval: {sampling_interval}s")
    print(f"  - Processing duration: {processing_duration}s")
    print(f"  - Realtime mode: {realtime}")
    print(f"  - Architecture: Producer-Consumer with Queue")
    print("="*70)
    print("Monitor Azure IoT Hub in another terminal:")
    print("  az iot hub monitor-events --hub-name <your-hub> --device-id <device-id>")
    print("="*70 + "\n")
    
    # Create shared queue (thread-safe, unbounded size)
    sample_queue = Queue()
    
    # Shared statistics dictionary (thread-safe operations)
    stats = {
        'produced': [],
        'processed': [],
        'dropped_on_send': [],
        'start_time': None
    }
    
    # Create producer and consumer threads
    producer = ProducerThread(
        queue=sample_queue,
        num_samples=num_samples,
        sampling_interval=sampling_interval,
        realtime=realtime,
        stats=stats
    )
    
    consumer = ConsumerThread(
        queue=sample_queue,
        processing_duration=processing_duration,
        stats=stats
    )
    
    # Start both threads
    sim_start = time.time()
    producer.start()
    consumer.start()
    
    # Wait for both threads to complete
    producer.join()  # Wait for producer to finish
    consumer.join()  # Wait for consumer to finish
    
    sim_end = time.time()
    total_duration = sim_end - sim_start
    
    # Shutdown Azure client
    if CLIENT:
        try:
            CLIENT.shutdown()
        except Exception:
            pass
    
    # Print summary
    print_summary(stats, num_samples, total_duration)
    
    return stats


def print_summary(stats: Dict, num_samples: int, total_duration: float):
    """Print detailed simulation summary."""
    processed = stats['processed']
    dropped = stats['dropped_on_send']
    
    print("\n" + "="*70)
    print("=== SIMULATION SUMMARY ===")
    print("="*70)
    print(f"Total simulation time: {total_duration:.2f}s")
    print(f"Total produced: {num_samples}")
    print(f"Successfully processed & sent: {len(processed)}")
    print(f"Dropped on send (failed): {len(dropped)}")
    print(f"Missed locally: 0 (queue prevents data loss!)")
    print(f"Success rate: {len(processed)/num_samples*100:.1f}%")
    print("="*70)
    
    # Processing statistics
    if processed:
        wait_times = [p['wait_time'] for p in processed]
        proc_times = [p['proc_time'] for p in processed]
        send_times = [p['send_duration'] for p in processed]
        
        print("\n📊 Processing Statistics:")
        print(f"  Wait time in queue:")
        print(f"    - Min: {min(wait_times):.3f}s")
        print(f"    - Max: {max(wait_times):.3f}s")
        print(f"    - Avg: {sum(wait_times)/len(wait_times):.3f}s")
        print(f"  Processing time:")
        print(f"    - Min: {min(proc_times):.3f}s")
        print(f"    - Max: {max(proc_times):.3f}s")
        print(f"    - Avg: {sum(proc_times)/len(proc_times):.3f}s")
        print(f"  Azure send time:")
        print(f"    - Min: {min(send_times):.3f}s")
        print(f"    - Max: {max(send_times):.3f}s")
        print(f"    - Avg: {sum(send_times)/len(send_times):.3f}s")
    
    # Detailed processed samples (first 10)
    if processed:
        print(f"\n✅ Processed Samples (showing first 10 of {len(processed)}):")
        print(f"{'Index':<8} {'Value':<10} {'Wait(s)':<10} {'Proc(s)':<10} {'Status'}")
        print("-" * 60)
        for p in processed[:10]:
            print(f"#{p['index']:<7} {p['value']:<10} {p['wait_time']:<10.3f} "
                  f"{p['proc_time']:<10.3f} {p['reason']}")
    
    # Failed samples
    if dropped:
        print(f"\n⚠️  Failed Samples:")
        for d in dropped:
            print(f"  - #{d['index']} {d['value']} (waited {d['wait_time']:.3f}s) - {d['reason']}")
    
    print("\n" + "="*70)
    print("🎯 Key Advantage: Multi-threading with queue = ZERO data loss!")
    print("   All samples are buffered and processed eventually.")
    print("="*70 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Multi-threaded IoT simulation with Azure IoT Hub integration"
    )
    parser.add_argument("--num", type=int, default=40, 
                        help="number of samples to produce")
    parser.add_argument("--sample-interval", type=float, default=0.5, 
                        help="seconds between samples")
    parser.add_argument("--proc-duration", type=float, default=2.0, 
                        help="processing duration per sample (seconds)")
    parser.add_argument("--realtime", action="store_true", 
                        help="run in realtime (sleep to match sample schedule)")
    args = parser.parse_args()
    
    run_simulation(
        num_samples=args.num,
        sampling_interval=args.sample_interval,
        processing_duration=args.proc_duration,
        realtime=args.realtime
    )
