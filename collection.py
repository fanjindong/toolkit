from functools import wraps

import redis_lock


def distributed_lock(key=None, indexs=None, expire=3):
    """
    redis分布式锁。
    :param index: [0,1,2] by args's index list
    :param keys: The (redis key) the lock should have.
    :param expire: The lock expiry time in seconds.
    :return:
    """

    def decorator(func):
        @wraps(func)
        def inner(*args, **kwargs):
            if key:
                primary = key
            elif indexs is None:
                primary = args
            elif isinstance(indexs, int):
                primary = args[indexs]
            elif isinstance(indexs, (list, tuple)):
                primary = [args[int(index)] for index in indexs]
            else:
                raise KeyError("invalid key and indexs")

            lock_key = 'actor:{}:{}'.format(func.__name__, primary)

            with redis_lock.Lock(redis, name=lock_key, expire=expire):
                return func(*args, **kwargs)

        return inner

    return decorator


def get_in(coll, path=None, default=None):
    """Returns a value at path in the given nested collection.
    Args:
        coll(object):
        path(str):'a.0.b.c'
    """
    if path is None:
        return coll

    for key in path.split('.'):
        try:
            if isinstance(coll, dict):
                coll = coll[key]
            elif isinstance(coll, list):
                coll = coll[int(key)]
            else:
                raise KeyError
        except (KeyError, IndexError, TypeError, ValueError):
            return default
    return coll


def iteritems(coll):
    return coll.items() if hasattr(coll, 'items') else coll


def merge_with(*dicts):
    """Merge several dicts."""
    dicts = list(dicts)
    if not dicts:
        return {}
    elif len(dicts) == 1:
        return dicts[0]

    lists = {}
    for c in dicts:
        for k, v in iteritems(c):
            lists[k] = v

    return lists

def transaction_lock(key=None):
    """
    The redlock algorithm is a distributed lock implementation for Redis
    :param key: default is userId
    :return:
    """

    def decorator(f):
        @wraps(f)
        async def decorated_function(request, *args, **kwargs):
            lock_manager = request.app.lock_manager
            logger = request.app.logger
            method = request.method

            if method == "GET":
                request_data = request.raw_args
            else:
                request_data = request.json or {}
            headers = request.headers
            if key is None:
                primary = headers['x-user-id']
            else:
                primary = request_data[key]

            union_key = 'lock:{}:{}'.format(f.__name__, primary)

            # you can use the lock as async context manager:
            try:
                async with await lock_manager.lock(union_key) as lock:
                    assert lock.valid is True
                    response = await f(request, *args, **kwargs)
                # assert lock.valid is False  # lock will be released by context manager
                return response
            except LockError:
                logger.warning((headers.get('x-request-id'), '{} Lock not acquired'.format(union_key)))
                return json({'status': 'Lock not acquired'}, 503)

        return decorated_function

    return decorator


def cached(cache_key, *, expire=10, bind_user=True, cache_status=False):
    """
    Decorator that caches the results of the function call
    :param cache_key: list 基于哪些请求参数，来命中缓存
    :param expire: int 缓存过期时间
    :param bind_user: bool 缓存是否针对用户，设为False则，仅针对参数识别缓存
    :param cache_status: bool 是否缓存response.status
    :return:
    """

    def decorator(f):
        @wraps(f)
        async def decorated_function(request, *args, **kwargs):
            redis = request.app.redis
            logger = request.app.logger
            method = request.method

            request_data = request.raw_args or {}
            request_data.update(request.json or {})

            headers = request.headers
            cache_values = [request_data.get(key, '') for key in cache_key]
            if bind_user:
                primary = str([headers.get('x-user-id', ''), *cache_values, args, kwargs])
            else:
                primary = str([*cache_values, args, kwargs])

            union_key = 'cached:{}:{}:{}:{}:{}'.format(
                request.app.config.SERVER_NAME,
                f.__module__,
                f.__name__,
                method,
                hashlib.md5(bytes(primary, 'utf-8')).hexdigest()
            )
            cached_data = await redis.execute('get', union_key)

            if cached_data:
                cache_response = loads(cached_data)
                if cache_status and "body" in cache_response and "status" in cache_response:
                    body = cache_response["body"]
                    status = cache_response["status"]
                else:
                    body = cache_response
                    status = 200
                return json(body, status=status)

            response = await f(request, *args, **kwargs)

            # response.body: bytes(json.dumps), response.status: int
            if not cache_status:
                cache_response = response.body
            else:
                cache_response = dumps({"body": loads(response.body), "status": response.status})
            await redis.execute('setex', union_key, expire, cache_response)

            return response

        return decorated_function

    return decorator
