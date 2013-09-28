import sys

class UnicodeMixin(object):
    """Boilerplate unicode mixin to save us from 2vs3 issues."""
    if sys.version_info[0] >= 3:
        def __str__(self):
            return self.__unicode__()
    else:
        def __str__(self):
            return self.__unicode__().encode('utf8')