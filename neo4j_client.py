import atexit

import neo4j

_uri = "neo4j://localhost:7687"
_user = "neo4j"
_passwd = "orient-adam-super-channel-genesis-8390"
_client = neo4j.GraphDatabase.driver(_uri, auth=(_user, _passwd))


def new_session():
    return _client.session()


def _close():
    _client.close()


atexit.register(_close)
