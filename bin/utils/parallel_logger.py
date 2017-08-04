import io, sys


class ParallelLogger:
    """
    This logger has 3 features:
      - It take both verbose and terse ("progress") logging, and outputs then as configured.
      - It logs verbose output to stdout and a logfile simultaneously.
      - Instead of being line-buffered, it buffers logfile output and only write it on flush().  This allows for
        verbose operation in a multi-threaded/process environment without interleaving output.
    """
    def __init__(self):
        self.destination = sys.stdout
        self.progress_string = ""
        self.terse = False
        self.quiet = False
        self.logfile = None

    def configure(self, logfile=None, terse=False, quiet=False):
        self.terse = terse
        self.quiet = quiet
        if logfile:
            self.logfile = io.TextIOWrapper(open(logfile, 'wb'))

    def output(self, msg, progress_char=None):
        if not self.quiet:
            sys.stdout.write(msg)
        if self.logfile:
            self.logfile.write(msg)
        if progress_char:
            self.progress(progress_char)

    def progress(self, char):
        self.progress_string += char

    def flush(self):
        if self.terse:
            sys.stdout.write(self.progress_string)
        self.progress_string = ""
        sys.stdout.flush()
        if self.logfile:
            self.logfile.flush()


logger = ParallelLogger()
