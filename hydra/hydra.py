#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import logging
import termbox
import utils
import click

from threading import Thread
from fuzzyfinder import fuzzyfinder
from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import Completer, Completion
from zkcli import Client, parse_auth
from conf import CONF_FILE, INIT_FILE, HIST_FILE
# import pdb, traceback, sys


logger = logging.getLogger(__file__)

operations = ('ls', 'add', 'create', 'delete', 'rmr', 'set',
              'setAcl', 'get', 'getAcl', 'initcfg', 'up')


def logger_conf(debug_mode):
    logger_fmt= '%(asctime)s %(name)s %(levelname)s %(message)s'
    if debug_mode:
        logging.basicConfig(level=logging.DEBUG, format=logger_fmt)
    else:
        logging.basicConfig(level=logging.INFO, format=logger_fmt)


def parser_config(data):
    host = data.get('server', '')
    auth = data.get('auth', '')

    logger.debug('Conf host is {}, auth is {}'.format(host, auth))

    # TODO If None in config file, raise Exception
    if not host:
        logger.warn('Invalid value in config file, plz check out.')
        raise RuntimeError

    # If raw_auth_data is None, login as anonymous
    acl, auth_data = parse_auth(auth)
    return host, acl, auth_data


class ZkCompleter(Completer):

    def __init__(self, completer):
        self._completer = completer

    def get_completions(self, document, complete_event):
        word_before_cursor = document.get_word_before_cursor(WORD=True)
        matches = fuzzyfinder(word_before_cursor, self._completer)
        for m in matches:
            yield Completion(m, start_position=-len(word_before_cursor))


def console(raw_data):
    try:
        host, acl, auth = parser_config(raw_data[1])
    except Exception:
        raise RuntimeError

    logger.debug('host {}, acl {}, auth {}'.format(host, acl, auth))

    with Client(INIT_FILE, host, default_acl=acl, auth_data=auth) as zk:
        zk.completer.extend(operations)
        t = Thread(target=zk.get_all_nodes)
        t.setDaemon(True)
        t.start()
        while True:
            cli_input = prompt(u"$[{}]>> ".format(raw_data[0]),
                               history=FileHistory(HIST_FILE),
                               auto_suggest=AutoSuggestFromHistory(),
                               completer=ZkCompleter(zk.completer))
            if not cli_input:
                continue
            elif cli_input == 'up':
                break
            else:
                cmds = cli_input.split()
                logger.debug('{}'.format(cmds))
                logger.debug('type of value is {}'.format(type(cmds[-1])))
                if cmds[0] in operations:
                    try:
                        getattr(zk, cmds[0])(*cmds[1:])
                    except Exception as e:
                        logger.error(e)
                        logger.warning('api {} call failed!!'.format(cmds[0]))
                        getattr(zk, 'usage')()
                else:
                    getattr(zk, 'usage')()


def main(conf, init):
    loader = utils.load_config(conf)
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
            except Exception as e:
                logger.warn(e)
                logger.warn('Something wrong happened, Quit gracefully...')
                break
        else:
            break


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
        length = len(msg)
        for i in range(length):
            c = spaceord
            if i < length:
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


@click.command()
@click.option('--debug', is_flag=True)
@click.option('-c', '--conf',
              default=CONF_FILE,
              help='Configuration of zookeeper server.')
@click.option('-i', '--init',
              default=INIT_FILE,
              help='Init configuration of zookeeper structure.')
def start(debug, conf, init):
    logger_conf(debug)
    main(conf, init)


if __name__ == '__main__':
    start()
