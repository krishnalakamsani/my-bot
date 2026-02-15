import time
import logging

import execution  # module import registers handlers and starts monitor thread


def _main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Execution runner started; module loaded and handlers registered")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("Execution runner interrupted; exiting")


if __name__ == '__main__':
    _main()
