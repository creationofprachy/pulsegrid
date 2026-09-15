#!/usr/bin/env python3
"""Convenience entry point: `python scripts/run_producer.py [--duration SEC]`"""
import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from producer.event_generator import run_producer

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=None, help="seconds to run, omit for infinite")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    logging.basicConfig(level=settings.log_level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    asyncio.run(run_producer(duration_sec=args.duration, seed=args.seed))
