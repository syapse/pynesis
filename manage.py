#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

import os

if __name__ == "__main__":
    sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)), "pynesis/tests"))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "testapp.settings")

    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
