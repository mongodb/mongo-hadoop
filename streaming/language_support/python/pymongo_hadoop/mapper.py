from .input import BSONInput, KeyValueBSONInput
from .output import BSONOutput, KeyValueBSONOutput

class BSONMapper(object):
    """Wraps BSONInput to allow writing mapper functions
    as generators.
    """

    def __init__(self, target, **kwargs):
        """`target` should be a generator function that accepts a
        single argument which will be an instance of :class:`BSONInput`,
        and which yields dictionaries to be emitted. The yielded
        dictionaries should conform to the format expected by
        :class:`BSONInput` (i.e. they should have the key defined
        in a field named `_id`).

        Keyword arguments are passed directly to the underlying
        :class:`BSONInput`.
        """

        output = BSONOutput()
        input = BSONInput(**kwargs)

        generator = target(input)
        for mapped in generator:
            output.write(mapped)

class KeyValueBSONMapper(object):
    """Wraps KeyValueBSONInput to allow writing mapper functions
    as generators.
    """

    def __init__(self, target, **kwargs):
        """`target` should be a generator function that accepts a
        single argument which will be an instance of
        :class:`KeyValueBSONInput`, and which yields tuples of
        (key, value) to be emitted.

        Keyword arguments are passed directly to the underlying
        :class:`KeyValueBSONInput`.
        """

        output = KeyValueBSONOutput()
        input = KeyValueBSONInput(**kwargs)

        generator = target(input)
        for key_and_value in generator:
            output.write(key_and_value)

