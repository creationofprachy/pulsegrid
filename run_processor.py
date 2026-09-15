#!/usr/bin/env python3
"""Convenience entry point: `python scripts/run_processor.py`"""
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from processor.stream_processor import StreamProcessor

if __name__ == "__main__":
    logging.basicConfig(level=settings.log_level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    processor = StreamProcessor()
    try:
        asyncio.run(processor.run())
    except KeyboardInterrupt:
        logging.getLogger("pulsegrid.processor").info("Shutdown requested (Ctrl+C)")
