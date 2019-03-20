import time
import datetime
import sys
import linecache
import traceback
class MyLogger:
    '''
    Class for logging
    '''

    info_tag = '[INFO]'
    warning_tag = '[WARNING]'
    error_tag = '[ERROR]'

    
    def __init__(self):
        self.messages = list()
        
    def output(self,message, **kwargs):
        print(message, **kwargs)
        
    def add_tag(self, message, tag):
        return '{0} {1}'.format(tag,message)
        
    def add_timestamp(self, message):
        time = datetime.datetime.now().strftime('[%H:%M:%S]')
        return self.add_tag(message, time)
    

    def info(self, message, **kwargs):
        message = self.add_timestamp(message)
        message = self.add_tag(message, self.info_tag)
        self.output(message, **kwargs)
        self.messages.append(message)
    
    def warning(self, message, **kwargs):
        message = self.add_timestamp(message)
        message = self.add_tag(message, self.warning_tag)
        self.output(message, **kwargs)
        self.messages.append(message)

    def error(self, message, **kwargs):
        message = self.add_timestamp(message)
        message = self.add_tag(message, self.error_tag)
        self.output(message, **kwargs)
        self.messages.append(message)

    def catch_exception(self, exc_note = None, include_traceback = False):
        
        # get exception info
        exc_type, exc_obj, tb_obj = sys.exc_info()
        frame = tb_obj.tb_frame
        line_number = tb_obj.tb_lineno
        filename = frame.f_code.co_filename
        linecache.checkcache(filename)
        line = linecache.getline(filename, line_number, frame.f_globals)
        
        # make string note
        info_string = 'Exception in  ({0}, Line {1} "{2}"): {3}'            .format(filename, line_number, line.strip(), exc_obj)

        if exc_note:
            info_string = "({0}) {1}".format(exc_note, info_string)

        # return full traceback
        if include_traceback:
            traceback_string = traceback.format_exc()
            info_string = "{0} \n\nFull tracback:\n{1}".format(exc_note, traceback_string)
        
        self.error(info_string)


    def list_warnings(self):
        return [msg for msg in self.messages if msg.startswith(self.warning_tag) or msg.startswith(self.error_tag)]
    
        
    def warnings_report(self):
        warnings = self.list_warnings()
        warnings_log = '\n'.join(warnings) 
        return "Routine completed with {0} warnings: \n\n{1}".format(len(warnings),warnings_log)

    def failure_message(self):
        logged_output = '\n'.join(self.messages)
        return "Routine failed; see logs:\n\n{}".format(logged_output)
    
    
    def live_sleeper(self,seconds):
        self.output('')
        for k in range(seconds):
            self.output('\r~{}~'.format(k+1), end='')
            time.sleep(1)
        self.output('\r')