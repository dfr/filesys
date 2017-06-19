#-
# Copyright (c) 2016-present Doug Rabson
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
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
