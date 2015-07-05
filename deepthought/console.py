"""This module provides an interface for users to interact with the program during execution"""
import logging
import cmd
import time
import os
import pprint
import app

import analyser


class Console(cmd.Cmd):
    """Start threads and allow users to interact with them

    Attributes:
        intro (str): The message displayed at the start
        prompt (str): The message that asks the user for a prompt
    """

    def __init__(self):
        """Initializes the console"""
        # Setup the logger
        self.logger = logging.getLogger(__name__)
        self.logger.warn("Console started")

        # Call parent constructor
        cmd.Cmd.__init__(self)

        # Init a pretty printer for formatting output
        self.pp = pprint.PrettyPrinter(indent=4)

        # Init the list of threads
        self.threads = app.threads

        self.intro = '\n\ndeepthought console - type ? for help\ntype exit to close program'
        self.prompt = '\ndeepthought > '

    def run(self):
        """Starts the threads, and then start the console"""
        # Wait for threads to start running before starting console
        time.sleep(2)
        self.cmdloop()

    def stop(self):
        """Stop the console"""
        self.logger.warn("Console shutting down")

    def do_EOF(self, line):
        """Exit the program when a EOF is inputted"""
        print "\nEOF detected, exiting program"
        self.do_exit("")

    @staticmethod
    def help_EOF():
        """Help message for EOF command"""
        print 'EOF\nEnds the program'

    def do_exit(self, line):
        """Gracefully exits the program

        Args:
            line (str): Arguments that might have been inputted by the user
        """
        # For each of the threads, check if they have a stop function
        # If they do, invoke it
        # Else, invoke the deconstructor
        for thread_name, thread in self.threads.iteritems():
            if hasattr(thread, 'stop'):
                thread.stop()
            else:
                thread.__del__()

        # Stop the console
        self.stop()

        # Exit the program
        os._exit(0)

    @staticmethod
    def help_exit():
        """Help message for exit command"""
        print 'exit\nGracefully exits the program'

    def do_crawler(self, line):
        """Displays the current status of the crawler

        Args:
            line (str): Arguments that might have been inputted by the user
        """
        status = self.threads['crawler'].status
        self.pp.pprint(status)

    @staticmethod
    def help_crawler():
        """Help message for crawler command"""
        print 'crawler\nDisplays the current status of the crawler'

    def do_analyse(self, file_path):
        """Analyses a given dir (containing tweets.csv and tps.csv)

        Args:
            file_path (str): The file path to the dir to be analysed
        """
        try:
            a = analyser.Analyser()
            a.analyse(file_path)
        except ValueError:
            print "Please check the file path provided ('" + file_path + "')"

    def do_ls(self, line):
        """Displays the files in the current dir

        Args:
            line (str): Arguments that might have been inputted by the user
        """
        self.pp.pprint(os.listdir("."))

    @staticmethod
    def help_analyse():
        """Help message for EOF command"""
        print "analyse <file_path>\n Analyses provided files"