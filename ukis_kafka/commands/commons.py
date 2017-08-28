# encoding: utf8

from .. import __version__ as ukis_kafka_version

import logging
import sys

import click

_loglevels = {
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG,
}


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(ukis_kafka_version)
    ctx.exit()

def loglevel_names():
    return _loglevels.keys()

def init_logging(levelname):
    level = _loglevels[levelname]
    root = logging.getLogger()
    root.setLevel(level)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    root.addHandler(ch)
