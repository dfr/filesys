#-
# Copyright (c) 2016-2017 Doug Rabson
# All rights reserved.
#

import os
import sys

def main():
    if len(sys.argv) < 3:
        print 'Usage: static-content.py <function> <file> ...'
        exit(1)

    fn = sys.argv[1]
    print "void %s(std::shared_ptr<RestRegistry> restreg) {" % fn;
    for file in sys.argv[2:]:
        assert(file.startswith("nfsd/ui"))
        f = open(file, 'r')
        data = f.read()
        f.close()
        print '    restreg->add("%s",' % file[7:]
        escaped = ''
        for byte in data:
            if byte == '\\':
                escaped += '\\\\'
            elif byte == '?':
                escaped += '\\?' # avoid trigraph issues
            elif byte == '"':
                escaped += '\\"'
            elif byte == '\a':
                escaped += '\\a'
            elif byte == '\b':
                escaped += '\\b'
            elif byte == '\f':
                escaped += '\\f'
            elif byte == '\n':
                escaped += '\\n\\\n'
            elif byte == '\r':
                escaped += '\\r'
            elif byte == '\t':
                escaped += '\\t'
            elif byte == '\v':
                escaped += '\\v'
            elif ord(byte) < 32:
                escaped += '\\' + hex(ord(byte))[1:]
            else:
                escaped += byte

        if file.endswith('.html'):
            contentType = 'text/html'
        elif file.endswith('.js'):
            contentType = 'application/javascript'
        elif file.endswith('.css'):
            contentType = 'text/css'
        else:
            contentType = 'text/plain'
            
        print '        "%s",' % escaped
        print '        "%s",' % contentType
        print '        %d);' % os.stat(file).st_mtime
    print "}"

if __name__ == '__main__':
    main()
