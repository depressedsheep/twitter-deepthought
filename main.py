"""Main module

This module initializes the app and starts it.
In the unlikely event it crashes, it will dump the stack trace and attempt to restart it.
"""

import traceback
import time

import deepthought.app


a = deepthought.app.App()

if __name__ == '__main__':
    while True:
        try:
            a.start()
        except:
            dump_file_path = time.strftime('%d-%m-%Y_%H_%M') + ".dump"
            div = "\n\n" + "-" * 50 + "\n\n"
            print div + "Fatal error occurred! Dumping stack trace to '" + dump_file_path + "'" + div
            traceback.print_exc(file=open(dump_file_path, 'wb'))
        else:
            break