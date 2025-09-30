#!/usr/bin/env python3
"""
azure_iot_single_thread.py

Single-threaded IoT simulation with real D2C sends to Azure IoT Hub.

Usage:
  # set env var with the device connection string obtained from `az iot hub device-identity connection-string show ...`
  export AZURE_DEVICE_CONN_STR="HostName=...;DeviceId=sim-device-1;SharedAccessKey=..."
  pip install azure-iot-device python-dotenv
  python azure_iot_single_thread.py --num 40 --sample-interval 0.5 --proc-duration 2.0
"""

import os
import time
import json
import random
import argparse
from typing import Tuple
from dotenv import load_dotenv
from azure.iot.device import IoTHubDeviceClient, Message

load_dotenv()

AZURE_CONN_STR = os.getenv("AZURE_DEVICE_CONN_STR")

if not AZURE_CONN_STR:
    raise ValueError("AZURE_DEVICE_CONN_STR environment variable is required. Please set it in your .env file.")

# Initialize Azure IoT Hub client
CLIENT = IoTHubDeviceClient.create_from_connection_string(AZURE_CONN_STR)
print("Azure IoT Device SDK: Connected successfully.")


def now_ts() -> str:
    return time.strftime("%X")


def send_to_azure(payload: dict) -> Tuple[bool, str]:
    """Send data to Azure IoT Hub. Blocking call."""
    try:
        msg = Message(json.dumps(payload))
        CLIENT.send_message(msg)
        return True, "sent"
    except Exception as e:
        return False, f"error:{e}"


def run_simulation(num_samples: int,
                   sampling_interval: float,
                   processing_duration: float,
                   realtime: bool):
    """
    Single-threaded loop. Sensor produces num_samples at start + i*sampling_interval.
    If processor is busy at sample arrival -> missed_local.
    If processor free -> start processing (blocks) and attempt send (real or simulated).
    """
    start_time = time.time()
    processing_end_time = start_time - 1.0  # when processor will be free
    processed = []
    missed_local = []
    dropped_on_send = []

    print("\n=== Single-threaded IoT D2C simulation ===")
    print(f"params: num={num_samples} sample_interval={sampling_interval}s proc_duration={processing_duration}s")
    print("Run the Azure monitor in another terminal to see what arrived at IoT Hub:")
    print("  az iot hub monitor-events --hub-name <your-hub> --device-id <device-id>\n")

    for i in range(num_samples):
        sample_time = start_time + i * sampling_interval
        sample_value = round(random.uniform(40.0, 95.0), 2)
        sample = {
            "device_id": "sim-device-1",
            "sample_index": i,
            "ts": sample_time,
            "noise_rms": sample_value
        }

        # Print produced (use virtual timestamp if not realtime)
        print(f"[{now_ts()}] PRODUCED sample #{i}: {sample_value}")

        # Check if processor is free at this sample's arrival
        if sample_time >= processing_end_time:
            # Processor is free: start processing at sample_time (we simulate this by blocking the thread)
            proc_start_real = time.time()
            proc_start_virtual = sample_time
            proc_end_virtual = proc_start_virtual + processing_duration
            print(f"[{now_ts()}] PROCESS START sample #{i}: {sample_value} (will finish at virtual {time.strftime('%X', time.localtime(proc_end_virtual))})")

            # Send to Azure IoT Hub (blocks the single thread)
            send_success, reason = send_to_azure(sample)
            send_end_real = time.time()

            # If the send itself took some time, ensure total processing time includes it:
            send_duration = send_end_real - proc_start_real
            remaining_proc = max(0.0, processing_duration - send_duration)
            if remaining_proc > 0:
                # block to simulate remaining processing work
                time.sleep(remaining_proc)

            # Update processing_end_time to virtual end (we model sensor producing on virtual schedule)
            processing_end_time = proc_end_virtual

            if send_success:
                processed.append((i, sample_value, sample_time, proc_start_virtual, proc_end_virtual, reason))
                print(f"[{now_ts()}] SENT sample #{i} -> cloud (reason={reason})")
            else:
                dropped_on_send.append((i, sample_value, sample_time, proc_start_virtual, reason))
                print(f"[{now_ts()}] ⚠️ SEND FAILED for sample #{i} -> dropped (reason={reason})")

        else:
            # Processor busy -> sample missed locally (vanished)
            busy_until = processing_end_time
            missed_local.append((i, sample_value, sample_time, busy_until))
            print(f"[{now_ts()}] ⚠️ MISSED sample #{i} (processor busy until {time.strftime('%X', time.localtime(busy_until))})")

        # If realtime mode set false, don't sleep (fast run); otherwise keep loop approximately realtime
        if realtime:
            # Wait until next scheduled sample time (but adapt for processing time and send delays)
            next_sample_at = start_time + (i + 1) * sampling_interval
            sleep_for = max(0.0, next_sample_at - time.time())
            if sleep_for > 0:
                time.sleep(sleep_for)

    # shutdown client if used
    if CLIENT:
        try:
            CLIENT.shutdown()
        except Exception:
            pass

    # Summary
    print("\n=== Simulation Summary ===")
    print(f"Total produced: {num_samples}")
    print(f"Processed & attempted send (stored count depends on IoT Hub monitor): {len(processed)}")
    print(f"Missed locally (vanished before processing): {len(missed_local)}")
    print(f"Dropped on send (attempted but failed): {len(dropped_on_send)}\n")

    if processed:
        print("Processed (index, value, virtual_arrival, proc_start->proc_end, send_reason):")
        for rec in processed:
            idx, val, arr, ps, pe, r = rec
            print(f" - #{idx} {val} [{time.strftime('%X', time.localtime(arr))}] {time.strftime('%X', time.localtime(ps))}->{time.strftime('%X', time.localtime(pe))} ({r})")

    if missed_local:
        print("\nMissed locally (index, value, arrival, busy_until):")
        for rec in missed_local:
            idx, val, arr, busy = rec
            print(f" - #{idx} {val} [{time.strftime('%X', time.localtime(arr))}] busy_until={time.strftime('%X', time.localtime(busy))}")

    if dropped_on_send:
        print("\nDropped on send (index, value, arrival, proc_start, reason):")
        for rec in dropped_on_send:
            idx, val, arr, ps, reason = rec
            print(f" - #{idx} {val} [{time.strftime('%X', time.localtime(arr))}] proc_start={time.strftime('%X', time.localtime(ps))} reason={reason}")

    return {
        "processed": processed,
        "missed_local": missed_local,
        "dropped_on_send": dropped_on_send
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num", type=int, default=40, help="number of samples to produce")
    parser.add_argument("--sample-interval", type=float, default=0.5, help="seconds between samples")
    parser.add_argument("--proc-duration", type=float, default=2.0, help="processing duration per sample (seconds)")
    parser.add_argument("--realtime", action="store_true", help="run in realtime (sleep to match sample schedule). Use for live demo.")
    args = parser.parse_args()

    # If user didn't choose realtime, the run will be as-fast-as-possible (useful for quick tests)
    run_simulation(num_samples=args.num,
                   sampling_interval=args.sample_interval,
                   processing_duration=args.proc_duration,
                   realtime=args.realtime)
