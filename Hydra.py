#!/usr/bin/env python
# encoding: utf-8

from kazoo.client import KazooClient
import logging
import readline
import yaml
import pdb, traceback, sys

# TODO add logger utility for py module
#  logging.basicConfig(level=logging.DEBUG)
FORMAT = '%(asctime)s %(name)s %(levelname)s %(message)s'
loggerM = logging.getLogger('Hydra.')
# loggerM.setLevel(logging.DEBUG)
loggerM.setLevel(logging.INFO)
handler = logging.StreamHandler()
fmt = logging.Formatter(FORMAT)
handler.setFormatter(fmt)
loggerM.addHandler(handler)
COMMAND_SET = ('ls', 'add', 'create', 'delete', 'rmr', 'set', 'setAcl', 'get', 'getAcl', 'initcfg', 'quit')
CONFIG_FILE = 'zkSerconfig.yaml'
INIT_FILE = 'zkInitconfig.yaml'


class Mykazoo(KazooClient):
    # TODO full command set just like ZooKeeper client
    def __init__(self, initfile, options, *args, **kwargs):
        self.login_info= initfile
        self.options = options
        self.current_candidates = []
        super(Mykazoo, self).__init__(*args, **kwargs)

    def ls(self, path):
        loggerM.debug('ls path is {}'.format(path))
        try:
            attr_list = self.get_children(path)
            attr_list.sort()
            for attr in attr_list:
                print(attr)
        except:
            loggerM.warn('{} try get_children wrong'.format(path))
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

    def delete(self, path):
        loggerM.info('delete path is {}'.format(path))
        if not self.exists(path):
            loggerM.warn("Znode {} is empty".format(path))
        else:
            super(Mykazoo, self).delete(path)

    def rmr(self, path):
        loggerM.info('rmr path is {}'.format(path))
        if not self.exists(path):
            loggerM.warn("Znode {} is empty".format(path))
        else:
            super(Mykazoo, self).delete(path, recursive=True)

    def create(self, path, value):
        loggerM.info('create path is {}'.format(path))
        if not self.exists(path):
            super(Mykazoo, self).create(path, value, makepath=True)
        else:
            loggerM.warn("Znode {} Already exists...".format(path))

    def add(self, path):
        loggerM.info('createAcl path is {} acl is {}'.format(path, self.default_acl))
        if not self.exists(path):
            self.ensure_path(path)
        else:
            loggerM.warn("Znode {} Already exists...".format(path))

    def initcfg(self):
        # TODO i wanna init some config for zookeeper from init.yaml
        loader = self._load_initconfig(self.login_info)
        print loader

    def _load_initconfig(self, file):
        with open(file, 'r') as f:
            return yaml.load(f)

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
                candidates = self._auto_completer(path)
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


def utility(raw_data):
    host, acl, auth = parser_config(raw_data)
    loggerM.debug('host {}, acl {}, auth {}'.format(host, acl, auth))
    zk = Mykazoo(INIT_FILE, COMMAND_SET, host, timeout=10, default_acl=acl, auth_data=auth)
    # TODO more Exception process need to accomplish eg. connect timeout
    try:
        loggerM.info('ZkServer is connecting...')
        zk.start()
        loggerM.info('ZkServer is connected!')
    except:
        loggerM.warn('ZkServer connect failed')
        return

    try:
        readline.parse_and_bind('tab: complete')
        readline.set_completer(zk.complete)
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


def load_config(file):
    with open(file, 'r') as f:
        return yaml.load(f)


def main():
    loader = load_config(CONFIG_FILE)
    # loop for zkServer selecet screen, i can change connection as i wish
    while True:
        # TODO check if Quit is selected let loop break
        # TODO check interrupt from Keyboard
        connData = interacter(loader)
        if connData:
            utility(connData)
        else:
            break


def parser_config(data):
    import kazoo.security as ks
    for k, v in data.iteritems():
        if k == 'server':
            host = v
        elif k == 'auth':
            raw_auth_data = v
        else:
            loggerM.warn('invalid k {}, v {} in the config data'.format(k, v))
            pass

    # TODO If None in config file, raise Exception
    if not host:
        loggerM.warn('Invalid value in config file, plz check out.')
        raise

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


# TODO convert to class, interacter login screen
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
        screen.addstr(sumline, defaultY, '> {}.{}\n'.format(sumline, title))
        dictMenu[sumline] = title
        sumline += 1
    screen.addstr(sumline, defaultY, '> {}.Quit'.format(sumline))
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
