import logging

class mortar_logging(object):
    mortar_log_level = logging.INFO

    @classmethod
    def set_log_level_error(cls):
        cls.mortar_log_level = logging.ERROR

    @classmethod
    def set_log_level_warn(cls):
        cls.mortar_log_level = logging.WARN

    @classmethod
    def set_log_level_info(cls):
        cls.mortar_log_level = logging.INFO

    @classmethod
    def set_log_level_debug(cls):
        cls.mortar_log_level = logging.DEBUG

def outputSchema(schema_str):
    def wrap(f):
        def wrapped_f(*args):
            return f(*args)
        return wrapped_f
    return wrap

def write_user_exception(filename, stream_err_output, num_lines_offset_trace=0):
    import sys
    import traceback
    import inspect
    (t, v, tb) = sys.exc_info()
    name = t.__name__
    record_error = False

    if name in ['SyntaxError', 'IndentationError']:
        syntax_error_values = v.args
        user_line_number = syntax_error_values[1][1] - num_lines_offset_trace
        error_message =  "%s: %s\n\tFile: %s, line %s column %s\n\t%s" % \
                                    (name,
                                     syntax_error_values[0],
                                     syntax_error_values[1][0],
                                     user_line_number,
                                     syntax_error_values[1][2],
                                     syntax_error_values[1][3])
    else:
        error_message = "%s: %s\n" % (name, v)
        user_line_number = None
        while 1:
            e_file_name = tb.tb_frame.f_code.co_filename
            if e_file_name.find(filename) > 0:
                record_error = True
            if not record_error:
                if not tb.tb_next:
                    break
                tb = tb.tb_next
                continue

            line_number = tb.tb_lineno
            mod = inspect.getmodule(tb)
            if mod:
                lines, offset = inspect.getsourcelines(mod)
                line = lines[line_number - offset - 1]
            else:
                #Useful to catch exceptions with an invalid module (like syntax
                #errors)
                lines, offset = inspect.getsourcelines(tb.tb_frame)
                if (line_number - 1) >= len(lines):
                    line = "Unknown Line"
                else:
                    line = lines[line_number - 1]

            user_line_number = line_number - num_lines_offset_trace
            func_name = tb.tb_frame.f_code.co_name
            error_message += 'File %s, line %s, in %s\n\t%s\n' % \
                                (e_file_name, user_line_number, func_name, line)
            if not tb.tb_next:
                break
            tb = tb.tb_next
    if user_line_number:
        stream_err_output.write("%s\n" % user_line_number)
    stream_err_output.write("%s\n" % error_message)
