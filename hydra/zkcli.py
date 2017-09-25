#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import logging

import utils
from kazoo.client import KazooClient

logger = logging.getLogger(__file__)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Client(KazooClient):
    # TODO full command set just like ZooKeeper client
    # TODO Client without initfile and options Client is KazooClient

    __metaclass__ = Singleton

    def __init__(self, initfile=None, *args, **kwargs):
        self.login_info = initfile
        self.completer = []
        super(Client, self).__init__(*args, **kwargs)

    def ls(self, path):
        logger.debug('ls path is {}'.format(path))
        try:
            attr_list = self.get_children(path)
            attr_list.sort()
            for attr in attr_list:
                print(attr)
        except Exception as e:
            logger.warn(e, '{} try get_children wrong'.format(path))
            pass

    def set(self, path, value):
        super(Client, self).set(path, value)

    def setAcl(self, path):
        super(Client, self).set_acls(path, self.default_acl)

    def get(self, path):
        print('{}'.format(super(Client, self).get(path)[0]))

    def getAcl(self, path):
        print('{}'.format(super(Client, self).get_acls(path)[0]))

    # TODO delete command support regular expression operations yep that is cool
    def delete(self, path):
        logger.info('delete path is {}'.format(path))
        if not self.exists(path):
            logger.warn("Znode {} is empty".format(path))
        else:
            super(Client, self).delete(path)

    def rmr(self, path):
        logger.info('rmr path is {}'.format(path))
        if not self.exists(path):
            logger.warn("Znode {} is empty".format(path))
        else:
            super(Client, self).delete(path, recursive=True)

    def create(self, path, value):
        logger.info('create path is {}'.format(path))
        if not self.exists(path):
            super(Client, self).create(path, value, makepath=True)
        else:
            logger.warn("Znode {} Already exists...".format(path))

    def add(self, path):
        logger.info('createAcl path is {} acl is {}'.format(
            path, self.default_acl))
        if not self.exists(path):
            self.ensure_path(path)
        else:
            logger.warn("Znode {} Already exists...".format(path))

    def initcfg(self):
        # TODO i wanna init some config for zookeeper from init.yaml
        if not self.login_info:
            logger.warn('Init file not specified, plz check out...')
            return

        loader = utils.load_config(self.login_info)
        print loader

    def usage(self):
        # TODO add color usage
        print("""Usage:
                 delete path
                 rmr path
                 create path data
                 add path
                 ls path
                 get path
                 getAcl path
                 set path data
                 setAcl path
                 initcfg InitConfig.sample.yaml
              """)

    def __enter__(self):
        try:
            logger.info('Zookeeper server is connecting...')
            self.start()
            logger.info('Zookeeper server is connected!')
            return self
        except Exception as e:
            logger.warn(e)
            logger.warn('ZooKeeper server connect failed!')
            self.close()
            logger.info('Connection is closed.')

    def __exit__(self, exc_type, exc_value, traceback):
        logger.info('Connection is closing.')
        logger.info('{} {} {}'.format(exc_type, exc_value, traceback))
        self.close()
        logger.info('Connection is closed.')

    def _get_nodes(self, root_path):
        self.completer.append(root_path)
        attr_list = self.get_children(root_path)
        if attr_list:
            for attr in attr_list:
                if root_path == '/':
                    new_root = root_path + attr
                else:
                    new_root = root_path + '/' + attr
                self._get_nodes(new_root)

    def get_all_nodes(self):
        self._get_nodes('/')


def parse_auth(auth):
    import kazoo.security as ks
    if auth:
        userpasswd = auth.split(':')
        acl = [ks.make_digest_acl(userpasswd[0], userpasswd[1], all=True)]
        auth_data = [('digest', auth)]
    else:
        acl = None
        auth_data = None

    logger.debug("acl: {}\nauth_data: {}".format(acl, auth_data))
    return acl, auth_data
