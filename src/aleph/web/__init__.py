import pprint
import time
from datetime import date, datetime, timedelta

import aiohttp_cors
import aiohttp_jinja2
import jinja2
import pkg_resources
from aiohttp import web
from aiohttp_apispec import (
    setup_aiohttp_apispec,
)

from aleph.web.controllers.routes import register_routes


def init_cors(app: web.Application):
    # Configure default CORS settings.
    cors = aiohttp_cors.setup(
        app,
        defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_methods=["GET", "POST"],
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
            )
        },
    )

    # Configure CORS on all routes.
    for route in list(app.router.routes()):
        if "/socket.io/" not in repr(route.resource):
            cors.add(route)


def create_aiohttp_app() -> web.Application:
    app = web.Application(client_max_size=1024**2 * 64)

    tpl_path = pkg_resources.resource_filename("aleph.web", "templates")
    jinja_loader = jinja2.ChoiceLoader(
        [
            jinja2.FileSystemLoader(tpl_path),
        ]
    )
    aiohttp_jinja2.setup(app, loader=jinja_loader)
    env = aiohttp_jinja2.get_env(app)
    env.globals.update(
        {
            "app": app,
            "date": date,
            "datetime": datetime,
            "time": time,
            "timedelta": timedelta,
            "int": int,
            "float": float,
            "len": len,
            "pprint": pprint,
        }
    )

    register_routes(app)

    init_cors(app)

    setup_aiohttp_apispec(
        app=app,
        title="Aleph",
        version="v0",
        url="/api/docs/json",
        swagger_path="/api/docs",
        swagger_ui_static_path="/api/docs/ui",
    )

    return app
