import logging


def start(config):
    level=config.get('loglevel')
    format=config.get('logformat')
    filename=config.get('logfile')
    filemode=config.get('logmode')
    handlers = [
        logging.FileHandler(
            filename=filename,
            mode=filemode
        )
    ]
    if config.get('logtocon', None):
        handlers = [logging.StreamHandler()]

    logging.basicConfig(
        level=level,
        format=format,
        handlers=handlers
    )

    return logging.getLogger()
