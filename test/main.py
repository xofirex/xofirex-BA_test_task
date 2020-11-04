import logging
import sys

from aiohttp import web

from .routers import setup_routes
from .settings import get_config


async def create_app(argv=None):
    app = web.Application()

    app['config'] = get_config(argv)

    setup_routes(app)

    return app


def main(argv):
    logging.basicConfig(level=logging.DEBUG)

    app = create_app(argv)

    config = get_config(argv)
    web.run_app(
        app,
        host=config['host'],
        port=config['port']
    )


if __name__ == '__main__':
    main(sys.argv[1:])
