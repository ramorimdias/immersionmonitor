#!/usr/bin/env python3
"""Stable entrypoint for the immersion bench monitor UI.

Existing launch scripts keep invoking ``readable_monitor.py``. The responsive
operator shell lives in ``monitor_responsive.py`` while acquisition and control
remain in the existing backend modules.
"""

from monitor_responsive import main


if __name__ == "__main__":
    main()
