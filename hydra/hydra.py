#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import logging
import readline
import termbox
import utils
import argparse
import click
import os

from kazoo.client import KazooClient
from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.contrib.completers import WordCompleter
# import pdb, traceback, sys

# TODO add logger utility for py module
# logging.basicConfig(level=logging.DEBUG)
FORMAT = '%(asctime)s %(name)s %(levelname)s %(message)s'
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
fmt = logging.Formatter(FORMAT)
handler.setFormatter(fmt)
logger.addHandler(handler)


CONFIG_FILE = './conf/zkSerconfig.yaml'
INIT_FILE = './conf/zkInitconfig.yaml'
OPT_HIS_LOG = './logs/cli_hist.txt'


def logger_conf(debug_mode):
    logger_fmt= '%(asctime)s %(name)s %(levelname)s %(message)s'
    if debug_mode:
        logging.basicConfig(level=logging.DEBUG, format=logger_fmt)
    else:
        logging.basicConfig(level=logging.INFO, format=logger_fmt)


ZkCompleter = WordCompleter([
    'ls', 'add', 'create', 'delete', 'rmr', 'set', 'setAcl',
    'get', 'getAcl', 'initcfg', 'up'], ignore_case=True)


class Mykazoo(KazooClient):
    # TODO full command set just like ZooKeeper client
    # TODO Mykazoo without initfile and options Mykazoo is KazooClient
    options = ('ls', 'add', 'create', 'delete', 'rmr', 'set', 'setAcl',
               'get', 'getAcl', 'initcfg', 'up')

    def __init__(self, initfile=None, *args, **kwargs):
        self.login_info = initfile
        self.current_candidates = []
        super(Mykazoo, self).__init__(*args, **kwargs)

    def ls(self, path):
        logger.debug('ls path is {}'.format(path))
        try:
            attr_list = self.get_children(path)
            attr_list.sort()
            for attr in attr_list:
                print(attr)
        except:
            logger.warn('{} try get_children wrong'.format(path))
            pass

    def _auto_completer(self, path):
        attr_list = self.get_children(path) or []
        return attr_list

    def set(self, path, value):
        super(Mykazoo, self).set(path, value)

    def setAcl(self, path):
        super(Mykazoo, self).set_acls(path, self.default_acl)

    def get(self, path):
        print('{}'.format(super(Mykazoo, self).get(path)[0]))

    def getAcl(self, path):
        print('{}'.format(super(Mykazoo, self).get_acls(path)[0]))

    # TODO delete command support regular expression operations yep that is cool
    def delete(self, path):
        logger.info('delete path is {}'.format(path))
        if not self.exists(path):
            logger.warn("Znode {} is empty".format(path))
        else:
            super(Mykazoo, self).delete(path)

    def rmr(self, path):
        logger.info('rmr path is {}'.format(path))
        if not self.exists(path):
            logger.warn("Znode {} is empty".format(path))
        else:
            super(Mykazoo, self).delete(path, recursive=True)

    def create(self, path, value):
        logger.info('create path is {}'.format(path))
        if not self.exists(path):
            super(Mykazoo, self).create(path, value, makepath=True)
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

    def _complete(self, text, state):
        if not self.options:
            return

        response = []
        if state == 0:
            origline = readline.get_line_buffer()
            begin = readline.get_begidx()
            end = readline.get_endidx()
            being_completed = origline[begin:end]

            logger.debug('origline={}'.format(repr(origline)))
            logger.debug('begin={}, end={}'.format(begin, end))

            if begin == 0:
                candidates = self.options
            else:
                idx = origline[0:begin + 1].find(' ')
                if idx:
                    path = origline[idx + 1:begin]
                logger.debug('path :{}'.format(path))
                candidates = self._auto_completer(path)
                logger.debug('candidates:'.format(candidates))

            if being_completed:
                self.current_candidates = \
                    [w for w in candidates if w.startswith(being_completed)]
            else:
                self.current_candidates = candidates

        try:
            response = self.current_candidates[state]
        except IndexError:
            response = None
        logger.debug('complete({}, {}) => {}'.format(
            repr(text), state, repr(response)))
        return response

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


def console(raw_data):
    try:
        host, acl, auth = parser_config(raw_data[1])
    except:
        raise RuntimeError

    logger.debug('host {}, acl {}, auth {}'.format(host, acl, auth))

    try:
        zk = Mykazoo(INIT_FILE, host, default_acl=acl, auth_data=auth)
    except:
        pass

    # TODO more Exception process need to accomplish eg. connect timeout
    try:
        logger.info('ZkServer is connecting...')
        zk.start()
        logger.info('ZkServer is connected!')
    except KeyboardInterrupt:
        raise KeyboardInterrupt
    except:
        logger.warn('ZkServer connect failed')
        return RuntimeError

    try:
        while 1:
            cli_input = prompt(u"$>> ",
                               history=FileHistory('./logs/cli_hist.txt'),
                               auto_suggest=AutoSuggestFromHistory(),
                               completer=ZkCompleter,)
            if not cli_input:
                continue
            elif cli_input == 'up':
                break
            else:
                print cli_input
    finally:
        zk.stop()


def main():
    loader = utils.load_config(CONFIG_FILE)
    while True:
        conn_data = interacter(loader)
        if conn_data is None:
            logger.info('Quit Hydra!')
            return

        if conn_data:
            try:
                console(conn_data)
            except KeyboardInterrupt:
                logger.warn('Interrupt From Keyboard, Quit...')
                break
            except:
                logger.warn('Something wrong happened, Quit gracefully...')
                break
        else:
            break


def parser_config(data):
    import kazoo.security as ks
    for k, v in data.iteritems():
        if k == 'server':
            host = v or ''
        elif k == 'auth':
            raw_auth_data = v or ''
        else:
            logger.warn('Invalid k {}, v {} in the config data'.format(k, v))
            pass

    logger.debug('Load data host is {}, raw_auth_data is {}'.format(
        host, raw_auth_data))
    # TODO If None in config file, raise Exception
    if not host:
        logger.warn('Invalid value in config file, plz check out.')
        raise RuntimeError

    # If raw_auth_data is None, login as anonymous
    if raw_auth_data:
        auth = []
        usrpasswd = raw_auth_data.split(':')
        acl = [ks.make_digest_acl(usrpasswd[0], usrpasswd[1], all=True)]
        auth.append(('digest', raw_auth_data))
    else:
        acl = None
        auth = None

    return host, acl, auth


class SelectBox(object):
    def __init__(self, tb, choices, active=-1):
        self.tb = tb
        tb.select_output_mode(2)
        self.active = active
        self.choices = choices
        self.color_active = (termbox.CYAN | termbox.BOLD, 0xec)
        self.color_normal = (termbox.CYAN, termbox.DEFAULT)
        self.color_arrow_active = (termbox.BLACK | termbox.BOLD, 0xec)
        self.color_arrow_normal = (termbox.BLACK, 0xec)

    def draw(self):
        for i, c in enumerate(self.choices):
            arrow_color = self.color_arrow_normal
            color = self.color_normal
            if i == self.active:
                arrow_color = self.color_arrow_active
                color = self.color_active
                header = '>'
            else:
                header = ' '
            content = ' {}.'.format(i) + c
            self._print_line(self.tb, header, 0, i, *arrow_color)
            self._print_line(self.tb, content, 1, i, *color)

    def validate_active(self):
        if self.active < 0:
            self.active = len(self.choices) - 1
        if self.active >= len(self.choices):
            self.active = 0

    def set_active(self, i):
        self.active = i
        self.validate_active()

    def move_up(self):
        self.active -= 1
        self.validate_active()

    def move_down(self):
        self.active += 1
        self.validate_active()

    def _print_line(self, tb, msg, x, y, fg, bg):
        spaceord = ord(u" ")
        l = len(msg)
        for i in range(l):
            c = spaceord
            if i < l:
                c = ord(msg[i])
            tb.change_cell(x + i, y, c, fg, bg)


def interacter(menu):
    with termbox.Termbox() as t:
        sb = SelectBox(t, menu, 0)
        t.clear()
        sb.draw()
        t.present()
        i = 0
        run_app = True
        while run_app:
            event_here = t.poll_event()
            while event_here:
                (type, ch, key, mod, w, h, x, y) = event_here
                if type == termbox.EVENT_KEY and key == termbox.KEY_ESC:
                    run_app = False
                    return None
                if type == termbox.EVENT_KEY:
                    if key == termbox.KEY_CTRL_J or key == termbox.KEY_ARROW_DOWN:
                        sb.move_down()
                    elif key == termbox.KEY_CTRL_K or key == termbox.KEY_ARROW_UP:
                        sb.move_up()
                    elif key == termbox.KEY_CTRL_U:
                        sb.set_active(0)
                    elif key == termbox.KEY_CTRL_D:
                        sb.set_active(len(sb.choices) - 1)
                    elif key == termbox.KEY_ENTER:
                        return (menu.items()[sb.active])
                event_here = t.peek_event()

            t.clear()
            sb.draw()
            t.present()
            i += 1


def parse():
    parser = argparse.ArgumentParser(
        description='Fuzzy finder and CLI for Zookeeper')
    parser.add_argument('-c', dest='zk_ser_conf',
                        help='Zookeeper login configuration',
                        type=str, default='zkSerconfig.yaml')
    parser.add_argument('-i', dest='zk_init_conf',
                        help='Zookeeper init configuration',
                        type=str, default='zkInitconfig.yaml')
    parser.add_argument('--version', action='version', version='%(prog)s 0.2')
    return parser.parse_args()


@click.command()
@click.option('--debug', is_flag=True)
def start(debug):
    pass


if __name__ == '__main__':
    main()
