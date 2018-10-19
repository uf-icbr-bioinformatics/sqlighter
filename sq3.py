#!/usr/bin/env python

import os
import sys
import atexit
import readline
import sqlite3 as sql

__version__ = "0.1"
__author__ = "Alberto Riva, ariva@ufl.edu"
__license__ = "GPLv3.0"

# TODO: .conn, to connect to multiple dbs at once
#       .set to change settings

def w(string, *args):
    sys.stderr.write(string.format(*args))

class Sqlite3Client():
    dbfilename = None
    db = None
    mode = 'def'      # or 'csv'
    outfile = None
    more = None
    aliases = {}
    settings = {}               # modified with the (forthcoming) `set' command.
    _initfile = "~/.sq3rc"
    _histfile = "~/.sq3hist"

    def __init__(self, dbfilename):
        self.dbfilename = dbfilename
        self.aliases = {}
        self.settings = {'histlen': 1000}
        self.loadHistory()

    def loadHistory(self):
        histpath = os.path.expanduser(self._histfile)
        readline.parse_and_bind("tab: complete")
        if hasattr(readline, "read_history_file"):
            try:
                readline.read_history_file(histpath)
            except IOError:
                pass
            atexit.register(self.saveHistory)

    def saveHistory(self):
        if 'histlen' in self.settings:
            readline.set_history_length(self.settings['histlen'])
        readline.write_history_file(os.path.expanduser(self._histfile))

    def loadInitFile(self):
        initpath = os.path.expanduser(self._initfile)
        if os.path.isfile(initpath):
            try:
                with open(initpath, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        self.interpret(line)
            except Exception as e:
                w("Error reading init file: {}\n", e)

    def main(self):
        w(";;; Sqlite3Client - v1.0 - (c) 2018, A. Riva\n")
        w(";;; Use .h or .help for help.\n")
        if not os.path.isfile(self.dbfilename):
            w(";;; Creating empty database {}\n".format(self.dbfilename))
        self.db = sql.connect(self.dbfilename)
        self.db.text_factory = str
        self.loadInitFile()
        try:
            while True:
                try:
                    cmd = raw_input("SQL> ").strip()
                except EOFError:
                    w("\n")
                    return
                if not cmd:
                    continue
                if self.interpret(cmd):
                    return
        finally:
            self.db.close()

    def interpret(self, cmd):
        words = cmd.split()
        if cmd[0] == ".":
            quit = self.doCommand(words)
            return quit
        elif words[0] in self.aliases:
            aliasdef = self.aliases[words[0]]
            newcmd = aliasdef.format(*words[1:])
            w("alias expanded to: {}\n", newcmd)
            self.interpret(newcmd)
        else:
            self.executeSQL(cmd)
            self.db.commit()
            return False

    def doCommand(self, words):
        key = words[0]
        if key in ['.q', '.quit']:
            return True
        elif key in ['.h', '.help']:
            self.showHelp()
        elif key in ['.m', '.mode']:
            self.setMode(words)
        elif key in ['.p', '.page']:
            self.setMore(words)
        elif key in ['.o', '.out']:
            self.setOutfile(words)
        elif key in ['.l', '.list']:
            self.showTables(words)
        elif key in ['.a', '.alias']:
            self.setAlias(words)
        elif key in ['.e', '.echo']:
            self.echo(words)
        elif key in ['.s', '.set']:
            pass                # implement set
        return False

    def showHelp(self):
        w("Commands start with a dot (.). Available commands:\n\n")
        w(".l[ist] [-full] [table...]  List all table names in db. With `-full', also display table structure.\n\n")
        w(".p[age] N                   When displaying query results, pause every N lines.\n")
        w("                            With no arguments, display current value of N. With -, disable paging.\n\n")
        w(".o[ut] O                    Write query results to file O in tab-delimited format.\n")
        w("                            With no arguments, display current value of O. With -, disable file output.\n\n")
        w(".m[ode] [def|csv]           Select default (human readable) or tab-delimited output format.\n")
        w("                            With no arguments, display current mode.\n\n")
        w(".a[lias] name definition    Set `name' as an alias for string `definition'. The definition may contain\n")
        w("                            Python-style placeholders {{0}}, {{1}}, etc. For example:\n")
        w("                              .alias nrows select count(*) from {{0}}\n")
        w("                            This can then be invoked as: SQL> nrows table_name\n")
        w("                            With no arguments, display all defined aliases.\n\n")
        w(".e[cho] words...            Print words... to standard output. Useful for messages in init file.\n\n")    
        w(".q[uit]                     Exit program (ctrl-d also works).\n\n")
        w("\nEverything else is interpreted as an SQL statement. The semicolon at the end of SQL statements is optional.\n")

    def echo(self, words):
        sys.stdout.write(" ".join(words[1:]) + "\n")

    def setAlias(self, words):
        if len(words) == 1:
            for aname in sorted(self.aliases.keys()):
                w("{} = {}\n", aname, self.aliases[aname])
        elif len(words) == 2:
            aname = words[1]
            if aname in self.aliases:
                w("{} = {}\n", aname, self.aliases[aname])
        else:
            aname = words[1]
            adef = " ".join(words[2:])
            self.aliases[aname] = adef

    def setMode(self, words):
        if len(words) == 1:
            w(";;; Mode: {}\n", self.mode)
        elif words[1] in ['def', 'csv']:
            self.mode = words[1]
        else:
            w(";;; Usage: .mode def|csv\n")            

    def setMore(self, words):
        if len(words) == 1:
            if self.more:
                w(";;; Page: {} lines\n", self.more)
            else:
                w(";;; Page: disabled\n")
        elif words[1] == "-":
            self.more = None
        else:
            try:
                m = int(words[1])
                self.more = m
            except ValueError:
                w(";;; Usage: .page number-of-rows\n")

    def setOutfile(self, words):
        if len(words) == 1:
            w(";;; Outfile: {}\n", self.outfile)
        elif words[1] == '-':
            self.outfile = None
            w(";;; Outfile: None\n")
        else:
            self.outfile = words[1]
            w(";;; Outfile set to {}\n", self.outfile)

    def showTables(self, words):
        full = False
        wanted = []
        for wo in words[1:]:
            if wo == '-full':
                full = True
            else:
                wanted.append(wo)

        curs = self.db.cursor()
        tabledefs = curs.execute("SELECT name, sql FROM sqlite_master WHERE type='table';").fetchall()
        for td in tabledefs:
            name = td[0]
            if not wanted or name in wanted:
                w("Table: {}\n", name)
                if full:
                    w("  Def: {}\n".format(td[1]))
                    idxdefs = curs.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='{}';".format(name))
                    for idx in idxdefs:
                        w("  Idx: {}\n".format(idx[0]))
            
    def executeSQL(self, cmd):
        if cmd[-1] != ";":
            cmd += ";"
        if sql.complete_statement(cmd):
            curs = self.db.cursor()
            try:
                curs.execute(cmd)
                self.showResults(curs)
                w(";;; Ok.\n")
            except sql.OperationalError as e:
                w(";;; SQL Error: {}\n", e)
            except sql.Warning as wa:
                w(";;; SQL Warning: {}\n", wa)
        else:
            w(";;; SQL syntax incorrect.\n")

    def showResults(self, curs):
        desc = curs.description # Query was not a SELECT, nothing to show.
        if desc is None:
            if curs.rowcount >= 0: # Don't show this for CREATE, etc.
                w(";;; {} row(s) affected.\n", curs.rowcount)
            return True

        if self.outfile:
            return self.showResultsFile(curs)

        if self.mode == 'csv':
            nrows = self.resultsToStream(curs, sys.stdout)
        else:
            nrows = self.resultsToScreen(curs)
        w(";;; {} row(s) returned.\n", nrows)
        return True

    def resultsToScreen(self, curs):
        fields = [ d[0] for d in curs.description ]
        nfields = range(len(fields))
        sizes = [ len(f) for f in fields ]
        top100 = curs.fetchmany(100)
        nrows = len(top100)
        for row in top100:
            for i in nfields:
                # srow = [ str(d) for d in row ]
                l = len(str(row[i]))
                sizes[i] = max(l, sizes[i])
        fpieces = [ "| {{:{}}} ".format(x) for x in sizes ]
        fstr = "".join(fpieces) + "|\n"
        hpieces = [ "+" + "-"*(l+2) for l in sizes ]
        hstr = "".join(hpieces) + "+\n"
        sys.stdout.write(hstr)
        sys.stdout.write(fstr.format(*fields))
        sys.stdout.write(hstr)
        nout = 0
        prevMore = None
        display = True
        for row in top100:
            if display:
                sys.stdout.write(fstr.format(*row))
            nout += 1
            if self.more and (nout % self.more) == 0:
                w("=== Row: {} ('enter for next page', 'q' to quit, 'e' to disable paging) ===", nout)
                ans = raw_input()
                if ans == "q" or ans == "Q":
                    display = False
                    prevMore = self.more
                    self.more = None
                elif ans == "e":
                    prevMore = self.more
                    self.more = None
        for row in curs:
            if display:
                sys.stdout.write(fstr.format(*row))
            nrows += 1
            nout += 1
            if self.more and (nout % self.more) == 0:
                w("=== Row: {} ('enter' for next page, 'q' to quit, 'e' to disable paging) ===", nout)
                ans = raw_input()
                if ans == "q" or ans == "Q":
                    display = False
                    prevMore = self.more
                    self.more = None
                elif ans == "e":
                    prevMore = self.more
                    self.more = None
        sys.stdout.write(hstr)
        if prevMore:
            self.more = prevMore
        return nrows

    def showResultsFile(self, curs):
        try:
            with open(self.outfile, "w") as out:
                nrows = self.resultsToStream(out)
            w(";;; {} row(s) written.\n", nrows)
            return True
        except IOError as e:
            w(";;; Error: {}\n", e)
            return False

    def resultsToStream(self, curs, out):
        nrows = 0
        out.write("\t".join([ d[0] for d in curs.description ]) + "\n")
        for row in curs:
            nrows += 1
            out.write("\t".join([ str(d) for d in row]) + "\n")
        return nrows

def main(args):
    databases = []
    ndb = 0
    for a in args:
        if a == "-h":
            return usage(True)
        elif a == "-v":
            sys.stdout.write("sq3.py v{}\n".format(__version__))
            return
        else:
            databases.append(a)
            ndb += 1

    if ndb == 0:
        return usage()
    elif ndb > 1:
        sys.stderr.write(";;; Warning: multi-database support is not implemented yet.\n")
    C = Sqlite3Client(databases[0])
    C.main()

def usage(full=False):
    sys.stdout.write("Usage: sq3.py [options] database.db\n")
    if full:
        sys.stdout.write("\nOptions:\n")
        sys.stdout.write(" -h   Display this help message.\n")
        sys.stdout.write(" -v   Display version number.\n")
        sys.stdout.write("\n")
        sys.stdout.write("At the SQL> prompt, type .h for help.\n")

if __name__ == "__main__":
    args = sys.argv[1:]
    if args:
        main(args)
    else:
        usage()

