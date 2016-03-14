for i in xrange(256):
    if (i & 15) == 0:
        print '       ',
    if i == 0:
        print '0,',
    else:
        for j in xrange(8):
            if i & (1 << j):
                print '%d,' % j,
                break
    if (i & 15) == 15:
        print
        
