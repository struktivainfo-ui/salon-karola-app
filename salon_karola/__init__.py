from salon_karola_legacy import app as _app
from salon_karola_legacy import boot_app


def create_app():
    return _app


__all__ = ["create_app", "boot_app"]
