#!/usr/bin/env python3
"""Stable entrypoint for the clean immersion bench monitor UI.

Existing launch scripts can keep invoking ``readable_monitor.py``. The actual
application now lives in ``monitor_app.py`` so the operator UI can evolve
without accumulating more compatibility code in the entrypoint.
"""

from monitor_app import main


if __name__ == "__main__":
    main()
