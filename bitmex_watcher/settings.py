from __future__ import absolute_import

import importlib
import os
import sys

from bitmex_watcher.utils.dotdict import DotDict
import bitmex_watcher.base_settings as base_settings


def import_path(fullpath):
    """
    Import a file with full path specification. Allows one to
    import from anywhere, something __import__ does not do.
    """
    path, filename = os.path.split(fullpath)
    filename, ext = os.path.splitext(filename)
    sys.path.insert(0, path)
    module = importlib.import_module(filename, path)
    importlib.reload(module)  # Might be out of date
    del sys.path[0]
    return module


try:
    user_settings = import_path(os.path.join('.', 'settings'))
except Exception:
    user_settings = None

# Assemble settings.
settings = {}
settings.update(vars(base_settings))
if user_settings:
    settings.update(vars(user_settings))

# Main export
settings = DotDict(settings)
