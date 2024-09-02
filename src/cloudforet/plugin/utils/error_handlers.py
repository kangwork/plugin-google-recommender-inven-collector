def handle_403_exception(default_response=[]):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "403" in str(e):
                    return default_response
                raise e
        return wrapper
    return decorator

