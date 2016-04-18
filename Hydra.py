#!/usr/bin/env python
# encoding: utf-8

from kazoo.client import KazooClient
import logging
import readline
import json

# TODO resolve the double logging problem
# TODO dig logging module
FORMAT='%(asctime)s %(name)s %(levelname)s %(message)s'
FORMAT1='%(levelname)s %(message)s'
#  logging.basicConfig(format=FORMAT1)
loggerM = logging.getLogger('Hydra')
#  loggerM.setLevel(logging.DEBUG)
loggerM.setLevel(logging.WARN)
handler = logging.StreamHandler()
loggerM.addHandler(handler)
COMMANDSET = ('ls', 'create', 'delete', 'set', 'init', 'up')
# TODO add a configure file for host
CONFIG_FILE = 'zkSerconfig.json'


class Mykazoo(KazooClient):
    def ls(self, path):
        loggerM.debug('ls path is {}'.format(path))
        try:
            attrList = self.get_children(path)
            for attr in attrList:
                value = self.get(path + '/' + attr)
                print('{}: {}'.format(attr, value[0]))
        except:
            pass

    def gls(self, path):
        attrList = self.get_children(path)
        loggerM.debug('gls got attrList {}'.format(attrList))
        return attrList

    def create(self, path):
        if not self.exists(path):
            self.ensure_path(path)
        else:
            print("Already exists...")

    def delete(self, path):
        if not self.exists(path):
            print "node is empty"
            return

        super(Mykazoo, self).delete(path, recursive=True)

    def set(self, path, value):
        super(Mykazoo, self).set(path, value)

    def init(self, path):
        # TODO i wanna init some config for zookeeper from config.json

        pass

    def usage(self):
        print("""Usage:
                 delete path
                 create path data
                 ls path
                 get path
                 set path data
                 init some data
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
                candidates = self.zkServer.gls(path)
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


def utility(host, auth=None):
    zk = Mykazoo(host, timeout=10)
    zk.start()
    # TODO there is some thing wrong with auth
    #  zk.add_auth('digest', auth)
    try:
        readline.parse_and_bind('tab: complete')
        readline.set_completer(MyCompleter(COMMANDSET, zk).complete)
        while True:
            cmd = raw_input('>>').split()
            if not cmd:
                continue
            elif cmd[0] == 'up':
                break
            elif cmd[0] in COMMANDSET:
                try:
                    getattr(zk, cmd[0])(cmd[1])
                except:
                    getattr(zk, 'usage')()
            else:
                getattr(zk, 'usage')()
    finally:
        zk.stop()


def main():
    connData = interacter(parseConfig())
    utility(connData['server'], connData['auth'])


def parseConfig():
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
        screen.addstr(sumline, defaultY, '>> {}.{}\n'.format(sumline, title))
        dictMenu[sumline] = title
        sumline += 1
    screen.addstr(sumline, defaultY, '>> {}.Quit'.format(sumline))
    dictMenu[sumline] = 'Quit'

    cursor = 0
    screen.move(cursor, cursorY)
    try:
        while True:
            event = screen.getch()
            if event == curses.KEY_ENTER or event == 10:
                if cursor == sumline:
                    break
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


def checkIllegal(host):
    return True

if __name__ == '__main__':
    main()
    #  parseConfig()
