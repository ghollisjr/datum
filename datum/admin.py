"""Admin panel dispatcher for datum.

Provides :admin <panel> [args] command support.  Each panel is a module
in datum.panels that returns structured data for display in Emacs.
"""

import json
from . import connect
from . import envelope
from . import printer


# Panel registry: name -> module-level get_data function
_panels = {}


def _ensure_panels():
    """Lazy-load panel modules."""
    if _panels:
        return
    from .panels import activity, databases, jobs, ssis
    _panels["activity"] = activity
    _panels["databases"] = databases
    _panels["jobs"] = jobs
    _panels["ssis"] = ssis


def run_panel(panel_name, driver, args):
    """Execute a panel query and send results via envelope.

    panel_name: one of 'activity', 'jobs', 'ssis'
    driver: the current dialect driver
    args: remaining command arguments (list of str)
    """
    _ensure_panels()

    if panel_name not in _panels:
        envelope.error(f":admin — unknown panel '{panel_name}'. "
                       f"Available: {', '.join(sorted(_panels))}")
        return

    module = _panels[panel_name]
    try:
        cursor = connect.get_connection().cursor()
        result = module.get_data(cursor, driver, args)
        envelope.admin_panel(result)
    except Exception as err:
        envelope.error(f":admin {panel_name} error: {err}")


def run_action(panel_name, action_name, driver, args):
    """Execute a panel action (e.g., kill session, start job).

    panel_name: panel that defines the action
    action_name: action identifier (e.g., 'kill', 'start-job')
    driver: the current dialect driver
    args: action arguments (list of str)
    """
    _ensure_panels()

    if panel_name not in _panels:
        envelope.error(f":admin-action — unknown panel '{panel_name}'")
        return

    module = _panels[panel_name]
    if not hasattr(module, 'run_action'):
        envelope.error(f":admin-action — panel '{panel_name}' has no actions")
        return

    try:
        cursor = connect.get_connection().cursor()
        module.run_action(cursor, driver, action_name, args)
    except Exception as err:
        envelope.error(f":admin {panel_name} {action_name} error: {err}")
