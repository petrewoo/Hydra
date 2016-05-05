#!/usr/bin/env python
# encoding: utf-8

from kazoo.client import KazooClient
import kazoo.security as ks
import logging
import readline
import json
import pdb, traceback, sys

#  logging.basicConfig(level=logging.DEBUG)
FORMAT = '%(asctime)s %(name)s %(levelname)s %(message)s'
loggerM = logging.getLogger('Hydra.')
#  loggerM.setLevel(logging.DEBUG)
loggerM.setLevel(logging.INFO)
handler = logging.StreamHandler()
fmt = logging.Formatter(FORMAT)
handler.setFormatter(fmt)
loggerM.addHandler(handler)
COMMAND_SET = ('ls', 'add', 'create', 'delete', 'rmr', 'set', 'setAcl', 'get', 'getAcl', 'init', 'quit')
CONFIG_FILE = 'zkSerconfig.json'


class Mykazoo(KazooClient):
    # TODO full command set just like ZooKeeper client
    def ls(self, *args):
        loggerM.debug('ls path is {}'.format(args[0]))
        try:
            attr_list = self.get_children(args[0])
            attr_list.sort()
            for attr in attr_list:
                print(attr)
        except:
            loggerM.warn('{} try get_children wrong'.format(args[0]))
            pass

    def _gls(self, *args):
        attr_list = self.get_children(args[0]) or []
        return attr_list

    def set(self, *args):
        super(Mykazoo, self).set(args[0], args[1])

    def setAcl(self, *args):
        super(Mykazoo, self).set_acls(args[0], self.default_acl)

    def get(self, *args):
        print('{}'.format(super(Mykazoo, self).get(args[0])[0]))

    def getAcl(self, *args):
        print('{}'.format(super(Mykazoo, self).get_acls(args[0])[0]))

    def delete(self, *args):
        loggerM.info('delete path is {}'.format(args[0]))
        if not self.exists(args[0]):
            loggerM.warn("Znode {} is empty".format(args[0]))
            return

        super(Mykazoo, self).delete(args[0])

    def rmr(self, *args):
        loggerM.info('rmr path is {}'.format(args[0]))
        if not self.exists(args[0]):
            loggerM.warn("Znode {} is empty".format(args[0]))
            return

        super(Mykazoo, self).delete(args[0], recursive=True)

    def create(self, *args):
        loggerM.info('create path is {}'.format(args[0]))
        if not self.exists(args[0]):
            super(Mykazoo, self).create(args[0], args[1], makepath=True)
        else:
            loggerM.warn("Znode {} Already exists...".format(args[0]))

    def add(self, *args):
        loggerM.info('createAcl path is {} acl is {}'.format(args[0], self.default_acl))
        if not self.exists(args[0]):
            self.ensure_path(args[0])
        else:
            loggerM.warn("Znode {} Already exists...".format(args[0]))

    def init(self, *args):
        # TODO i wanna init some config for zookeeper from config.json
        pass

    def usage(self):
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
                 init zk from config.json
              """)


class MyCompleter(object):
    def __init__(self, options, zkServer):
        self.options = options
        self.zkServer = zkServer
        self.current_candidates = []
        return

    def complete(self, text, state):
        response = []
        if state == 0:
            origline = readline.get_line_buffer()
            begin = readline.get_begidx()
            end = readline.get_endidx()
            being_completed = origline[begin:end]

            loggerM.debug('origline={}'.format(repr(origline)))
            loggerM.debug('begin={}, end={}'.format(begin, end))

            if begin == 0:
                candidates = self.options
            else:
                idx = origline[0:begin + 1].find(' ')
                if idx:
                    path = origline[idx + 1:begin]
                loggerM.debug('path :{}'.format(path))
                candidates = self.zkServer._gls(path)
                loggerM.debug('candidates:'.format(candidates))

            if being_completed:
                self.current_candidates = [w for w in candidates if w.startswith(being_completed)]
            else:
                self.current_candidates = candidates

        try:
            response = self.current_candidates[state]
        except IndexError:
            response = None
        loggerM.debug('complete({}, {}) => {}'.format(repr(text), state, repr(response)))
        return response


def utility(host, raw_auth_data=None, theme='digest'):
    usrpasswd = raw_auth_data.split(':')
    acl = [ks.make_digest_acl(usrpasswd[0], usrpasswd[1], all=True)]
    zk = Mykazoo(host, timeout=10, default_acl=acl, auth_data=[(theme, raw_auth_data)])
    zk.start()
    try:
        readline.parse_and_bind('tab: complete')
        readline.set_completer(MyCompleter(COMMAND_SET, zk).complete)
        while True:
            cmd = raw_input('>> ').split()
            if not cmd:
                continue
            elif cmd[0] == 'up':
                break
            elif cmd[0] in COMMAND_SET:
                try:
                    getattr(zk, cmd[0])(*cmd[1:])
                except:
                    type, value, tb = sys.exc_info()
                    traceback.print_exc()
                    pdb.post_mortem(tb)
                    loggerM.warn('api {} call failed!!'.format(cmd[0]))
                    getattr(zk, 'usage')()
            else:
                getattr(zk, 'usage')()
    finally:
        zk.stop()


def main():
    # TODO input zkconfig.json as param return index of config
    # draw login screen based on return value of func load_config
    # add func parser_config to parse the index get the connect data of zk
    # draw a interacter screen(login) is very important
    connData = interacter(load_config())
    if connData:
        utility(connData['server'], connData['auth'])


def parser_config(data):
    pass


def load_config():
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)


# TODO convert to class
def interacter(menu):
    import curses
    defaultY = 0
    cursorY = 3
    screen = curses.initscr()
    screen.border(1, 20, 1, 20)
    curses.noecho()
    curses.curs_set(1)
    screen.keypad(1)
    screen.erase()

    sumline = 0
    dictMenu = {}
    for title in menu:
        screen.addstr(sumline, defaultY, 'Hydra >> {}.{}\n'.format(sumline, title))
        dictMenu[sumline] = title
        sumline += 1
    screen.addstr(sumline, defaultY, 'Hydra >> {}.Quit'.format(sumline))
    dictMenu[sumline] = 'Quit'

    cursor = 0
    screen.move(cursor, cursorY)
    try:
        while True:
            event = screen.getch()
            if event == curses.KEY_ENTER or event == 10:
                if cursor == sumline:
                    return False
                else:
                    return menu[dictMenu[cursor]]
            elif event == curses.KEY_UP:
                if cursor == 0:
                    cursor = sumline
                else:
                    cursor -= 1
            elif event == curses.KEY_DOWN:
                if cursor < sumline:
                    cursor += 1
                else:
                    cursor = 0
            screen.move(cursor, cursorY)
            screen.refresh()
    finally:
        curses.endwin()

if __name__ == '__main__':
    main()
