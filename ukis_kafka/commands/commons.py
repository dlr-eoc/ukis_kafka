# encoding: utf8

from .. import __version__ as ukis_kafka_version

import logging
import sys

import click
import yaml

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

def init_logging(levelname, logfile=None):
    level = _loglevels[levelname]
    root = logging.getLogger()
    root.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    if logfile == None: # log to stdout
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        ch.setFormatter(formatter)
        root.addHandler(ch)
    else:
        ch = logging.RotatingFileHanlder(logfile, maxBytes=10*1024*1024, backupCount=10)
        ch.setLevel(level)
        ch.setFormatter(formatter)
        root.addHandler(ch)

class ConfigurationError(Exception):
    pass

class Configuration(object):
    cfg = {}

    def __init__(self, cfg):
        self.cfg = cfg

    def yaml_read(self, fh):
        self.cfg.update(yaml.load(fh.read()))

    def yaml_dumps(self):
        return yaml.dump(self.cfg, default_flow_style=False)

    def get(self, keys, default=None, required=True):
        '''get values from the configuration and raise meaningful errors when
        something goes wrong'''
        v = {}
        v.update(self.cfg) # make a copy
        for i in range(len(keys)):
            try:
                if type(v[keys[i]]) != dict and i != (len(keys)-1):
                    raise TypeError
                v = v[keys[i]]
            except (KeyError, TypeError):
                if default is not None or required == False:
                    return default
                raise ConfigurationError('Configuration setting {0} is not set'.format('.'.join(keys)))
        return v or default