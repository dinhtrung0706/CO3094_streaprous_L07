#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course.
#
# StreapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#

from collections.abc import MutableMapping


class CaseInsensitiveDict(MutableMapping):
    """The :class:`CaseInsensitiveDict<MutableMapping>` object, which
    contains a custom behavior of MutuableMapping.

    Usage::

      >>> import tools
      >>> word = CaseInsensitiveDict(status_code='404', msg="Not found")
      >>> code = word['status_code']
      >>> code
      404

      >>> msg = word['msg']
      >>> s.send(r)
      Not found

      >>> print(word)
      {'status_code': '404', 'msg': 'Not found'}

    """

    def __init__(self, *args, **kwargs):
        self._data = {}
        self._display = {}
        # Keep the former attribute as an alias for callers that used it.
        self.store = self._data
        self.update(*args, **kwargs)

    def __getitem__(self, key):
        return self._data[key.lower()]

    def __setitem__(self, key, value):
        lower_key = key.lower()
        if lower_key not in self._data:
            self._display[lower_key] = key
        self._data[lower_key] = value

    def __delitem__(self, key):
        lower_key = key.lower()
        del self._data[lower_key]
        del self._display[lower_key]

    def __iter__(self):
        return (self._display[key] for key in self._data)

    def __len__(self):
        return len(self._data)

    def copy(self):
        """Return an independent mapping with original key casing preserved."""
        return type(self)(self.items())

    def __repr__(self):
        return f"{type(self).__name__}({dict(self.items())!r})"
