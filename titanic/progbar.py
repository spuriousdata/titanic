import sys, os

rows, cols = os.popen('stty size', 'r').read().split()
cols = int(cols)

def update(t, c=None):
    if c is None:
        n = t
        start = " %2d%% [" % (n * 100)
    else:
        n = float(c)/float(t)
        start = "(%d/%d) %2d%% [" % (c, t, n * 100)
    end =  "]\r"
    blocks = (cols - (len(start) + len(end) - 1))
    mid = "=" * (int(blocks * n) - 1)
    sys.stdout.write(start + mid + ">" + " " * (blocks - (len(mid)+1)) + end)
    sys.stdout.flush()


def finish():
    print()
